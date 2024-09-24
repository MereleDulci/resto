package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/MereleDulci/jsonapi"
	"github.com/MereleDulci/resto"
	"github.com/MereleDulci/resto/pkg/action"
	"github.com/MereleDulci/resto/pkg/resource"
	"github.com/MereleDulci/resto/pkg/typecast"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Authenticator func(*http.Request) *resource.Token
type ErrorHandler func(w http.ResponseWriter, r *http.Request, suggestedCode int, err error) error
type Middleware func(http.HandlerFunc) http.HandlerFunc

func NewHandler(handlers map[string]*resto.ResourceHandle, authenticator Authenticator, errHandler ErrorHandler, errchan chan error) Handler {
	return Handler{
		handlers:      handlers,
		mw:            make([]Middleware, 0),
		authenticator: authenticator,
		errhandler:    errHandler,
		errchan:       errchan,
	}
}

type Handler struct {
	handlers      map[string]*resto.ResourceHandle
	mw            []Middleware
	authenticator Authenticator
	errhandler    ErrorHandler
	errchan       chan error
}

func (h Handler) Use(mw ...Middleware) Handler {
	h.mw = append(h.mw, mw...)
	return h
}

func (h Handler) composeMw(next http.HandlerFunc) http.HandlerFunc {
	for i := len(h.mw) - 1; i >= 0; i-- {
		next = h.mw[i](next)
	}
	return next
}

func (h Handler) AttachMux(ns string, mux *http.ServeMux) Handler {
	if h.authenticator == nil {
		panic(errors.New("request authenticator is not configured"))
	}
	if h.errchan == nil {
		panic(errors.New("error feedback chan is no configured"))
	}

	for resource, rh := range h.handlers {
		baseUrl := "/" + strings.Trim(resource, "/")
		if ns != "" {
			baseUrl = fmt.Sprintf("/%s/%s", strings.Trim(ns, "/"), strings.Trim(baseUrl, "/"))
		}

		mux.HandleFunc("HEAD "+baseUrl, h.composeMw(h.makeMetaHandler(rh)))
		mux.HandleFunc("GET "+baseUrl, h.composeMw(h.makeFindManyHandler(rh)))
		mux.HandleFunc("GET "+baseUrl+"/{id}", h.composeMw(h.makeFindOneHandler(rh)))
		mux.HandleFunc("POST "+baseUrl, h.composeMw(h.makeCreateHandler(rh)))
		mux.HandleFunc("PATCH "+baseUrl+"/{id}", h.composeMw(h.makeUpdateHandler(rh)))
		mux.HandleFunc("DELETE "+baseUrl+"/{id}", h.composeMw(h.makeDeleteHandler(rh)))

		for _, collAction := range rh.Actions.CollectionActions {
			mux.HandleFunc(collAction.Config.Method+" "+baseUrl+"/actions/"+collAction.Config.Name,
				h.composeMw(h.makeCallHandler(rh, collAction)),
			)
		}
		for _, singleAction := range rh.Actions.SingleActions {
			mux.HandleFunc(
				singleAction.Config.Method+" "+baseUrl+"/{id}/"+singleAction.Config.Name,
				h.composeMw(h.makeCallOnHandler(rh, singleAction)),
			)
		}
	}
	return h
}

func (h Handler) makeMetaHandler(rh *resto.ResourceHandle) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		query := resource.Query{}
		query.Filter = pickFilterQueries(r.URL.Query())
		query.Limit = pickInt64WithDefault(r.URL.Query(), "page[limit]", rh.Defaults().Limit())
		query.Skip = pickInt64WithDefault(r.URL.Query(), "page[offset]", 0)

		meta, err := rh.Meta(r.Context(), resource.NewReq().WithToken(h.authenticator(r)).WithQuery(query))
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		w.Header().Set("Content-Range", fmt.Sprintf("# %d-%d/%d", query.Skip, query.Skip, meta.Count))

		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func (h Handler) makeFindManyHandler(rh *resto.ResourceHandle) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		query := resource.Query{}
		query.Filter = pickFilterQueries(r.URL.Query())
		query.Include = pickCommaSeparated(r.URL.Query(), "include")
		query.Sort = pickCommaSeparated(r.URL.Query(), "sort")
		query.Limit = pickInt64WithDefault(r.URL.Query(), "page[limit]", rh.Defaults().Limit())
		query.Skip = pickInt64(r.URL.Query(), "page[offset]")

		results, err := rh.Find(r.Context(), resource.NewReq().WithToken(h.authenticator(r)).WithQuery(query))

		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		buf, err := jsonapi.MarshalMany(results)
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		if r.URL.Query().Has("page[offset]") {
			meta, err := rh.Meta(r.Context(), resource.NewReq().WithToken(h.authenticator(r)).WithQuery(query))
			if err != nil {
				handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
				if handleErr != nil {
					h.errchan <- handleErr
				}
				return
			}

			w.Header().Set("Content-Range", fmt.Sprintf("# %d-%d/%d", query.Skip, query.Skip+int64(len(results)), meta.Count))
			buf, err = jsonapi.MixInMeta(buf, meta.Map())
			if err != nil {
				handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
				if handleErr != nil {
					h.errchan <- handleErr
				}
				return
			}
		}

		w.Header().Set("Content-Type", jsonapi.MediaType)
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(buf); err != nil {
			h.errchan <- err
		}
	}
}

func (h Handler) makeFindOneHandler(rh *resto.ResourceHandle) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		query := resource.Query{}
		query.Filter = pickFilterQueries(r.URL.Query())
		query.Include = pickCommaSeparated(r.URL.Query(), "include")
		query.Sort = pickCommaSeparated(r.URL.Query(), "sort")
		query.Limit = pickInt64WithDefault(r.URL.Query(), "page[limit]", rh.Defaults().Limit())
		query.Skip = pickInt64(r.URL.Query(), "page[offset]")

		query.Filter["id"] = r.PathValue("id")

		results, err := rh.Find(r.Context(), resource.NewReq().WithToken(h.authenticator(r)).WithQuery(query).WithId(r.PathValue("id")))
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		if len(results) == 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		buf, err := jsonapi.Marshal(results[0])
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
		w.Header().Set("Content-Type", jsonapi.MediaType)
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(buf); err != nil {
			h.errchan <- err
		}
	}
}

func (h Handler) makeCreateHandler(rh *resto.ResourceHandle) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := resource.Query{}
		query.Filter = pickFilterQueries(r.URL.Query())
		query.Include = pickCommaSeparated(r.URL.Query(), "include")
		query.Sort = pickCommaSeparated(r.URL.Query(), "sort")
		query.Limit = pickInt64WithDefault(r.URL.Query(), "page[limit]", rh.Defaults().Limit())
		query.Skip = pickInt64(r.URL.Query(), "page[offset]")

		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		payload, err := jsonapi.UnmarshalManyAsType(rawBody, rh.GetResourceReflectType())
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		resourcesToCreate := make([]resource.Resourcer, len(payload))
		for i, p := range payload {
			resourcesToCreate[i] = p.(resource.Resourcer)
		}

		result, err := rh.Create(r.Context(), resource.NewReq().WithToken(h.authenticator(r)).WithQuery(query).WithPayload(resourcesToCreate))
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		buf, err := jsonapi.Marshal(result)
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		w.Header().Set("Content-Type", jsonapi.MediaType)
		w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
		w.WriteHeader(http.StatusCreated)
		if _, err := w.Write(buf); err != nil {
			h.errchan <- err
		}
	}
}

func (h Handler) makeUpdateHandler(rh *resto.ResourceHandle) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := resource.Query{}
		query.Filter = pickFilterQueries(r.URL.Query())
		query.Include = pickCommaSeparated(r.URL.Query(), "include")
		query.Sort = pickCommaSeparated(r.URL.Query(), "sort")
		query.Limit = pickInt64WithDefault(r.URL.Query(), "page[limit]", rh.Defaults().Limit())
		query.Skip = pickInt64(r.URL.Query(), "page[offset]")

		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		patch := make([]typecast.PatchOperation, 0)
		if err := json.Unmarshal(rawBody, &patch); err != nil {
			handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		resource, err := rh.Update(r.Context(), resource.NewReq().WithToken(h.authenticator(r)).WithId(r.PathValue("id")).WithQuery(query).WithPayload(patch))
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		buf, err := jsonapi.Marshal(resource)
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		w.Header().Set("Content-Type", jsonapi.MediaType)
		w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(buf); err != nil {
			h.errchan <- err
		}
	}
}

func (h Handler) makeDeleteHandler(rh *resto.ResourceHandle) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := rh.Delete(r.Context(), resource.NewReq().WithToken(h.authenticator(r)).WithId(r.PathValue("id"))); err != nil {
			handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func (h Handler) makeCallHandler(rh *resto.ResourceHandle, act action.CollectionRegistryEntry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := resource.Query{}
		query.Filter = pickFilterQueries(r.URL.Query())
		query.Include = pickCommaSeparated(r.URL.Query(), "include")
		query.Sort = pickCommaSeparated(r.URL.Query(), "sort")
		query.Limit = pickInt64WithDefault(r.URL.Query(), "page[limit]", rh.Defaults().Limit())
		query.Skip = pickInt64(r.URL.Query(), "page[offset]")

		req := resource.NewReq().
			WithMethod(r.Method).
			WithToken(h.authenticator(r)).
			WithQuery(query)

		if act.Config.Method == "POST" {
			rawBody, err := io.ReadAll(r.Body)
			if err != nil {
				handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
				if handleErr != nil {
					h.errchan <- handleErr
				}
				return
			}

			payload, err := jsonapi.UnmarshalManyAsType(rawBody, act.Config.PayloadType)
			if err != nil {
				handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
				if handleErr != nil {
					h.errchan <- handleErr
				}
				return
			}

			req = req.WithPayload(payload)
		}

		results, err := rh.Call(r.Context(), req, act.Config.Name)
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		buf, err := jsonapi.MarshalMany(results)
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		w.Header().Set("Content-Type", jsonapi.MediaType)
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(buf); err != nil {
			h.errchan <- err
		}
	}
}

func (h Handler) makeCallOnHandler(rh *resto.ResourceHandle, act action.SingleRegistryEntry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := resource.Query{}
		query.Filter = pickFilterQueries(r.URL.Query())
		query.Include = pickCommaSeparated(r.URL.Query(), "include")
		query.Sort = pickCommaSeparated(r.URL.Query(), "sort")
		query.Limit = pickInt64WithDefault(r.URL.Query(), "page[limit]", rh.Defaults().Limit())
		query.Skip = pickInt64(r.URL.Query(), "page[offset]")

		query.Filter["id"] = r.PathValue("id")

		req := resource.NewReq().
			WithMethod(r.Method).
			WithToken(h.authenticator(r)).
			WithQuery(query).
			WithId(r.PathValue("id"))

		if act.Config.Method == "POST" {
			rawBody, err := io.ReadAll(r.Body)
			if err != nil {
				handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
				if handleErr != nil {
					h.errchan <- handleErr
				}
				return
			}

			payload, err := jsonapi.UnmarshalManyAsType(rawBody, act.Config.PayloadType)
			if err != nil {
				handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
				if handleErr != nil {
					h.errchan <- handleErr
				}
				return
			}

			req = req.WithPayload(payload)
		}

		results, err := rh.CallOn(r.Context(), req, act.Config.Name)
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusBadRequest, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		if len(results) == 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		buf, err := jsonapi.MarshalMany(results)
		if err != nil {
			handleErr := h.errhandler(w, r, http.StatusInternalServerError, err)
			if handleErr != nil {
				h.errchan <- handleErr
			}
			return
		}

		w.Header().Set("Content-Type", jsonapi.MediaType)
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(buf); err != nil {
			h.errchan <- err
		}
	}
}

func pickFilterQueries(q url.Values) map[string]string {
	out := map[string]string{}
	for k, _ := range q {
		if strings.HasPrefix(k, "filter[") && strings.HasSuffix(k, "]") {
			out[strings.TrimSuffix(strings.Replace(k, "filter[", "", 1), "]")] = q.Get(k)
		}
	}
	return out
}

func pickCommaSeparated(q url.Values, key string) []string {
	if q.Has(key) {
		v := q.Get(key)
		return strings.Split(v, ",")
	}
	return []string{}
}

func pickInt64(q url.Values, key string) int64 {
	if q.Has(key) {
		v := q.Get(key)
		val, err := strconv.Atoi(v)
		if err != nil {
			return 0
		}
		return int64(val)
	}
	return 0
}

func pickInt64WithDefault(q url.Values, key string, def int64) int64 {
	if q.Has(key) {
		v := q.Get(key)
		val, err := strconv.Atoi(v)
		if err != nil {
			return def
		}
		return int64(val)
	}
	return def
}
