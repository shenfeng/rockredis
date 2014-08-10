package main

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	//	"regexp"
	"strconv"
	"strings"
)

// 10g, 4m to int value
func memtoll(s string) (int, error) {
	mul := 1
	s = strings.ToLower(s)

	if strings.Contains(s, "k") {
		mul = 1024
	} else if strings.Contains(s, "m") {
		mul = 1024 * 1024
	} else if strings.Contains(s, "g") {
		mul = 1024 * 1024 * 1024
	}

	end := len(s)

	for i, b := range s {
		if b > '9' || b < '0' {
			end = i
			break
		}
	}

	if i, err := strconv.Atoi(s[:end]); err == nil {
		return i * mul, nil
	} else {
		return 0, err
	}
}

func set(cfg interface{}, field, value string) error {
	pValue := reflect.ValueOf(cfg)
	if pValue.Kind() != reflect.Ptr || pValue.Elem().Kind() != reflect.Struct {
		panic(fmt.Errorf("config must be a pointer to a struct"))
	}

	//
	field = strings.Replace(field, "-", "", -1)

	v := pValue.Elem().FieldByNameFunc(func(s string) bool {
		return strings.ToLower(s) == strings.ToLower(field)
	})

	if v.IsValid() {
		switch v.Type().Kind() {
		case reflect.Int:
			if i, err := memtoll(value); err == nil {
				// fmt.Println(i)
				v.SetInt(int64(i))
			} else {
				return err
			}
		case reflect.String:
			v.SetString(value)
		}
	}

	return nil
}

func ReadCfg(dst interface{}, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "#") && len(line) > 1 {
			if ps := strings.Split(line, " "); len(ps) == 2 {
				set(dst, strings.TrimSpace(ps[0]), strings.TrimSpace(ps[1]))
			}
		}
	}

	pValue := reflect.ValueOf(dst).Elem()
	for i := 0; i < pValue.NumField(); i++ {
		f := pValue.Field(i)
		name := reflect.TypeOf(dst).Elem().Field(i).Name
		switch f.Type().Kind() {
		case reflect.Int:
			if f.Int() == 0 {
				return fmt.Errorf("missing config for %v", name)
			}
		case reflect.String:
			if f.String() == "" {
				return fmt.Errorf("missing config for %v", name)
			}
		}
	}

	return nil
}
