package confighelper

import (
	"fmt"
	"strings"

	"github.com/beego/beego/v2/core/config"
)

//section configer enhancer

type SectionConfig struct {
	//given incref a global access of other configurations
	Configer config.Configer
	//either given a section name from Configer to fetch
	SectionName string
	//or passed in a ConfigMap for boostrap
	ConfigMap map[string]string
}

func isSectionName(s string) bool {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "[") {
		return false
	}
	if !strings.HasSuffix(s, "]") {
		return false
	}
	return true
}

func getSectionName(s string) string {
	s = strings.TrimLeft(s, "[")
	return strings.TrimRight(s, "]")
}

func GetSectionHasRef(configer config.Configer, sectionName string) (map[string]string, error) {
	ret := make(map[string]string)
	seen := make(map[string]bool)
	if err := getSectionHasRef(configer, sectionName, ret, seen, ""); err != nil {
		return nil, err
	}

	return ret, nil
}

func getSectionHasRef(configer config.Configer, sectionName string, ret map[string]string, seen map[string]bool, prefix string) error {

	if b, ok := seen[sectionName]; ok && b {
		return nil
	}
	seen[sectionName] = true
	m, err := configer.GetSection(sectionName)
	if err != nil {
		return fmt.Errorf(`GetSection[%s]:%s`, sectionName, err.Error())
	}
	for k, v := range m {
		if !isSectionName(v) {
			//TODO prefix it
			key := k
			if prefix != "" {
				key = fmt.Sprintf(`%s.%s`, prefix, k)
			}
			ret[key] = v
			continue
		}
		nextSectionName := getSectionName(v)
		//recursive fun
		nextPrefix := k
		if nextSectionName == k {
			nextPrefix = ""
		}
		if err := getSectionHasRef(configer, nextSectionName, ret, seen, nextPrefix); err != nil {
			return err
		}

	}

	return nil
}
