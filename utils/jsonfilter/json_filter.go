package jsonfilter

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

var (
	KEYS = `$filters` //where the list of json paths defines
	SEP  = `;`
)

func SetKEYS(s string) {
	KEYS = s
}
func SetSEP(s string) {
	SEP = s
}

func compareEqualFn(s, s1 string) bool {
	fmt.Println(`compareEqualFn `, s, s1)
	if s == "*" {
		return true
	}
	return s == s1
}

//EvaluateString simply compare the string value of json path. no array supported
func EvaluateString(cfg map[string]string, body []byte) bool {
	_paths, ok := cfg[KEYS]
	if !ok {
		fmt.Println(`no $filter`)
		return false
	}
	pathKeys := strings.Split(_paths, SEP)
	if len(pathKeys) == 0 {

		return false
	}

	for _, k := range pathKeys {
		_allowedValues, ok := cfg[k]
		if !ok {
			__allowedValues, ok2 := cfg[strings.ToLower(k)]
			if !ok2 {
				fmt.Println(k, `is not seen`)
				return false
			}
			_allowedValues = __allowedValues

		}

		allowedValues := strings.Split(_allowedValues, SEP)
		v := gjson.Get(string(body), k)
		sv := v.String()

		found := false
		for _, _v := range allowedValues {
			if compareEqualFn(_v, sv) {
				found = true
				break
			}
		}
		if !found {

			return false
		}

	}

	return true

}
