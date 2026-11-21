package message

import (
	"strings"
	"unicode"
)

// searchTokens builds unigram/bigram tokens for substring-friendly search.
// Mongo $text cannot tokenize Chinese well; this multikey field can.
func searchTokens(text string) []string {
	text = strings.ToLower(text)
	runes := make([]rune, 0, len(text))
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			runes = append(runes, r)
		}
	}
	if len(runes) == 0 {
		return nil
	}
	tokens := make([]string, 0, len(runes)*2)
	for i, r := range runes {
		tokens = append(tokens, string(r))
		if i+1 < len(runes) {
			tokens = append(tokens, string(runes[i:i+2]))
		}
	}
	return tokens
}
