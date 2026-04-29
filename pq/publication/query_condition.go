package publication

import (
	"fmt"
	"regexp"
	"strings"
)

var queryConditionUnsafeKeywordPattern = regexp.MustCompile(
	`(?i)\b(DROP|ALTER|CREATE|TRUNCATE|INSERT|UPDATE|DELETE|GRANT|REVOKE|COPY|EXECUTE|DO)\b`,
)

var queryConditionUnsafeSequences = []string{
	";",
	"--",
	"/*",
	"$$",
}

func ValidateQueryCondition(cond string) error {
	if strings.TrimSpace(cond) == "" {
		return nil
	}
	for _, seq := range queryConditionUnsafeSequences {
		if strings.Contains(cond, seq) {
			return fmt.Errorf("queryCondition must not contain %q", seq)
		}
	}
	if m := queryConditionUnsafeKeywordPattern.FindString(cond); m != "" {
		return fmt.Errorf("queryCondition must not contain keyword %q", strings.TrimSpace(m))
	}
	return nil
}
