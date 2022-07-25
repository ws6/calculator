package confighelper

//string_helpers.go provide common way search string from the config

import (
	"regexp"
	"strings"
)

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func StringInRegexSlice(a string, list []string) bool {
	for _, pattern := range list {

		matched, _ := regexp.MatchString(pattern, a)
		if matched {
			return true
		}

	}
	return false
}

func (cfg *SectionConfig) GetConfigArray(key string) []string {
	valStr, ok := cfg.ConfigMap[key]
	if !ok {
		return nil
	}
	_sp := strings.Split(valStr, ",")
	sp := []string{}
	for _, v := range _sp { //!!there could be empty
		sp = append(sp, strings.TrimSpace(v))
	}
	return sp

}

type SearchStrInArrayFn func(string, []string) bool

//IsInArray return if "search" parameter is in the key's values sep by comma
func (cfg *SectionConfig) IsIn(key, search string, fn SearchStrInArrayFn) bool {
	array := cfg.GetConfigArray(key)
	if len(array) == 0 {
		return false
	}
	return fn(search, array)
}

func (cfg *SectionConfig) IsInOrNotSet(key, search string, fn SearchStrInArrayFn) bool {
	array := cfg.GetConfigArray(key)
	if len(array) == 0 {
		return true //!!!this is not set
	}
	return fn(search, array)
}

func (cfg *SectionConfig) IsInArray(key, search string) bool {
	return cfg.IsIn(key, search, StringInSlice)
}
func (cfg *SectionConfig) IsInRegexArray(key, search string) bool {
	return cfg.IsIn(key, search, StringInRegexSlice)
}

func (cfg *SectionConfig) IsInArrayOrNotSet(key, search string) bool {
	return cfg.IsInOrNotSet(key, search, StringInSlice)
}

func (cfg *SectionConfig) IsInRegexArrayOrNotSet(key, search string) bool {
	return cfg.IsInOrNotSet(key, search, StringInRegexSlice)
}
