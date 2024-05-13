package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/MereleDulci/jsonapi"
	"github.com/MereleDulci/resto"
	"github.com/MereleDulci/resto/pkg/access"
	"github.com/MereleDulci/resto/pkg/collection"
	"github.com/MereleDulci/resto/pkg/req"
	"github.com/MereleDulci/resto/pkg/typecast"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func NewHandler(handlers map[string]*resto.ResourceHandle, authenticator func(*http.Request) *access.Token, errchan chan error) Handler {
	return Handler{
		handlers,
		authenticator,
		errchan,
	}
}

type Handler struct {
	handlers      map[string]*resto.ResourceHandle
	authenticator func(*http.Request) *access.Token
	errchan       chan error
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

		mux.HandleFunc("GET "+baseUrl, h.makeFindManyHandler(rh))
		mux.HandleFunc("GET "+baseUrl+"/{id}", h.makeFindOneHandler(rh))
		mux.HandleFunc("POST "+baseUrl, h.makeCreateHandler(rh))
		mux.HandleFunc("PATCH "+baseUrl+"/{id}", h.makeUpdateHandler(rh))
		mux.HandleFunc("DELETE "+baseUrl+"/{id}", h.makeDeleteHandler(rh))
	}
	return h
}

func (h Handler) makeFindManyHandler(rh *resto.ResourceHandle) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		r.Context()

		internalContext := req.NewCtx()

		internalContext.SetAuthentication(h.authenticator(r))

		query := internalContext.Query()
		query.Filter = pickFilterQueries(r.URL.Query())
		query.Include = pickCommaSeparated(r.URL.Query(), "include")
		query.Sort = pickCommaSeparated(r.URL.Query(), "sort")
		query.Limit = pickInt64(r.URL.Query(), "limit")
		query.Skip = pickInt64(r.URL.Query(), "skip")

		results, err := rh.Find(internalContext, query)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
			}
			return
		}

		buf, err := jsonapi.MarshalMany(results)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
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

func (h Handler) makeFindOneHandler(rh *resto.ResourceHandle) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		internalContext := req.NewCtx()

		internalContext.SetAuthentication(h.authenticator(r))

		query := internalContext.Query()
		query.Filter = pickFilterQueries(r.URL.Query())
		query.Include = pickCommaSeparated(r.URL.Query(), "include")
		query.Sort = pickCommaSeparated(r.URL.Query(), "sort")
		query.Limit = pickInt64(r.URL.Query(), "limit")
		query.Skip = pickInt64(r.URL.Query(), "skip")

		query.Filter["id"] = r.PathValue("id")
		internalContext.SetId(r.PathValue("id"))

		results, err := rh.Find(internalContext, query)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
			}
			return
		}

		if len(results) == 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		buf, err := jsonapi.Marshal(results[0])
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
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
		internalContext := req.NewCtx()
		internalContext.SetAuthentication(h.authenticator(r))

		query := internalContext.Query()
		query.Filter = pickFilterQueries(r.URL.Query())
		query.Include = pickCommaSeparated(r.URL.Query(), "include")
		query.Sort = pickCommaSeparated(r.URL.Query(), "sort")
		query.Limit = pickInt64(r.URL.Query(), "limit")
		query.Skip = pickInt64(r.URL.Query(), "skip")

		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
			}
			return
		}

		payload, err := jsonapi.UnmarshalManyAsType(rawBody, rh.GetResourceReflectType())
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
			}
			return
		}

		resourcesToCreate := make([]collection.Resourcer, len(payload))
		for i, p := range payload {
			resourcesToCreate[i] = p.(collection.Resourcer)
		}

		result, err := rh.Create(internalContext, resourcesToCreate)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
			}
			return
		}

		buf, err := jsonapi.Marshal(result)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
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
		internalContext := req.NewCtx()
		internalContext.SetAuthentication(h.authenticator(r))

		query := internalContext.Query()
		query.Filter = pickFilterQueries(r.URL.Query())
		query.Include = pickCommaSeparated(r.URL.Query(), "include")
		query.Sort = pickCommaSeparated(r.URL.Query(), "sort")
		query.Limit = pickInt64(r.URL.Query(), "limit")
		query.Skip = pickInt64(r.URL.Query(), "skip")

		internalContext.SetId(r.PathValue("id"))

		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
			}
			return
		}

		patch := make([]typecast.PatchOperation, 0)
		if err := json.Unmarshal(rawBody, &patch); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
			}
			return
		}

		resource, err := rh.Update(internalContext, r.PathValue("id"), patch, query)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
			}
			return
		}

		buf, err := jsonapi.Marshal(resource)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
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
		internalContext := req.NewCtx()

		internalContext.SetAuthentication(h.authenticator(r))
		internalContext.SetId(r.PathValue("id"))

		if err := rh.Delete(internalContext, r.PathValue("id")); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, err := io.WriteString(w, err.Error())
			if err != nil {
				h.errchan <- err
			}
			return
		}

		w.WriteHeader(http.StatusNoContent)
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
