package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

type fetcher struct {
	client http.Client
	token  string
}

type Response struct {
	Response FieldResponse `json:"response"`
}

type FieldResponse struct {
	Count int      `json:"count"`
	Items []Entity `json:"items"`
}

type Entity struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}

const (
	maxCitiesCountPerRequest = 1000
)

func (f *fetcher) GetRussianCities(ctx context.Context, needAll bool, offset int) ([]Entity, error) {
	const urlCities = "https://api.vk.com/method/database.getCities?country_id=1&v=5.103"

	u, err := url.Parse(urlCities)
	if err != nil {
		panic(fmt.Errorf("failed to parse cities url: %w", err))
	}

	q := u.Query()
	q.Set("access_token", f.token)

	if needAll {
		q.Set("need_all", "1")
	}

	q.Set("count", strconv.Itoa(maxCitiesCountPerRequest))
	q.Set("offset", strconv.Itoa(offset))

	u.RawQuery = q.Encode()

	var resp Response
	if err := f.fetch(ctx, u.String(), &resp); err != nil {
		return nil, err
	}

	return resp.Response.Items, nil
}

const (
	maxUniCountPerRequest = 10000
)

func (f *fetcher) GetUniversities(ctx context.Context, city, offset int) ([]Entity, error) {
	const urlCities = "https://api.vk.com/method/database.getUniversities?v=5.103"

	u, err := url.Parse(urlCities)
	if err != nil {
		panic(fmt.Errorf("failed to parse cities url: %w", err))
	}

	q := u.Query()
	q.Set("access_token", f.token)

	q.Set("count", strconv.Itoa(maxUniCountPerRequest))
	q.Set("offset", strconv.Itoa(offset))

	q.Set("city_id", strconv.Itoa(city))

	u.RawQuery = q.Encode()

	var resp Response
	if err := f.fetch(ctx, u.String(), &resp); err != nil {
		return nil, err
	}

	return resp.Response.Items, nil
}

func (f *fetcher) fetch(ctx context.Context, reqURL string, res interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)

	return dec.Decode(res)
}
