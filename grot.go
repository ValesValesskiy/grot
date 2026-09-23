// При большом количестве роутов попробовать сравнить с разделением роутов на массивы по методам(число) внутри которых они разложены по длиннам
// сегментов урла, может быть ускорится

//может добавлю кеш-настройку как опциональный при инициализации для конкретных роутов, или буду проверять, что если в урле нету параметров, то можно положить в карту урлы - ноды, можно даже при инициализации, тогда и сканер не трогать,
//а кеш ответов добавлю тоже опциональный или через обработчик с возвращением ключа, потому что надо бы в обработчике и на токен ориентироваться или на контекст, а это в осноном в статичных роутах и происходит
// ключ кешей МЕТОД:УРЛА
// А можн и генерировать ключ отдельным обработчиком...может быть, по перформансу надо смотреть

// Можно попрообовать всё-таки использовать keyedMutex через ключи-инты(uint64), а не строки(предварительно строку преобразовывать в число на лету)
// Почитать про false sharing, подумать над тем, чтобы не блочить мьютекст при подсчёте читателей и писателей в KeyedMutex, ShardedMutex, чтобы не блочить запросы пока читается кеш
// или придумать как распараллелить это не теряя в скорости, читать из другого источника который не блочится и сбрасывается при протухании...

package grot

import (
	"bufio"
	"bytes"
	"cmp"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"unsafe"
)

var hexTable [256]byte

func init() {
	for i := range hexTable {
		hexTable[i] = 0
	}

	for i := '1'; i <= '9'; i++ {
		hexTable[i] = byte(i - '0')
	}
	for i := 'a'; i <= 'f'; i++ {
		hexTable[i] = byte(i - 'a' + 10)
	}
	for i := 'A'; i <= 'F'; i++ {
		hexTable[i] = byte(i - 'A' + 10)
	}
}

func unhex(b byte) byte {
	return hexTable[b]
}

// func unhex(b byte) byte {
// 	switch {
// 		case '1' <= b && b <= '9': return b - '0'
// 	case 'a' <= b && b <= 'f': return b - 'a' + 10
// 	case 'A' <= b && b <= 'F': return b - 'A' + 10
// 	}

// 	return 0
// }

func unescapeString(b []byte, out []byte) (string, int) {
	wIdx := 0
	for rIdx := 0; rIdx < len(b); rIdx++ {
		if b[rIdx] == '%' && rIdx+2 < len(b) {
			out[wIdx] = (unhex(b[rIdx+1]) << 4) | unhex((b[rIdx+2]))
			rIdx += 2
		} else {
			out[wIdx] = b[rIdx]
		}
		wIdx++
	}

	if wIdx == 0 {
		return "", 0
	}

	return unsafe.String(&out[0], wIdx), wIdx
}

type segment struct {
	start, end int
}

type pathScanner struct {
	path           []byte
	pathSegments   [32]segment
	pathCount      int
	querySegments  [64]segment
	queryCount     int
	unescapeBuffer [512]byte
	unescapeCursor int
}

func (s *pathScanner) Scan(path []byte) {
	s.path = path
	s.pathCount = 0
	s.queryCount = 0
	start := 0
	isQuery := false
	isQueryName := true
	for i := 0; i <= len(path); i++ {
		if isQuery {
			if isQueryName {
				if i == len(path) || path[i] == '=' {
					if s.queryCount*2 < len(s.querySegments) {
						s.querySegments[s.queryCount*2] = segment{start: start, end: i}

						start = i + 1
						isQueryName = false
					}
				}
			} else {
				if i == len(path) || path[i] == '&' {
					if s.queryCount*2 < len(s.querySegments) {
						s.querySegments[s.queryCount*2+1] = segment{start: start, end: i}
						s.queryCount++

						start = i + 1
						isQueryName = true
					}
				}
			}
		} else {
			if i == len(path) || path[i] == '/' || path[i] == '?' {
				if i > start && s.pathCount < len(s.pathSegments) {
					s.pathSegments[s.pathCount] = segment{start: start, end: i}
					s.pathCount++
				}

				start = i + 1

				if i != len(path) && path[i] == '?' {
					isQuery = true
				}
			}
		}
	}
}

func (s *pathScanner) Get(index int) string {
	seg := s.pathSegments[index]

	return unsafe.String(&s.path[seg.start], seg.end-seg.start)
}

func (s *pathScanner) GetUnescaped(index int) string {
	seg := s.pathSegments[index]

	bytes := s.path[seg.start:seg.end]
	un, w := unescapeString(bytes, s.unescapeBuffer[s.unescapeCursor:])
	s.unescapeCursor += w

	return un
}

func (s *pathScanner) GetQuery(index int) [2]string {
	seg := s.querySegments[index*2 : index*2+2]

	return [2]string{unsafe.String(&s.path[seg[0].start], seg[0].end-seg[0].start), unsafe.String(&s.path[seg[1].start], seg[1].end-seg[1].start)}
}

func (s *pathScanner) GetQueryUnescaped(index int) [2]string {
	seg := s.querySegments[index*2 : index*2+2]

	bytes := [2][]byte{s.path[seg[0].start:seg[0].end], s.path[seg[1].start:seg[1].end]}

	un1, w := unescapeString(bytes[0], s.unescapeBuffer[s.unescapeCursor:])
	s.unescapeCursor += w

	un2, w := unescapeString(bytes[1], s.unescapeBuffer[s.unescapeCursor:])
	s.unescapeCursor += w

	return [2]string{un1, un2}
}

func (s *pathScanner) SegmentLen(index int) int {
	seg := s.pathSegments[index]

	return seg.end - seg.start
}

func (s *pathScanner) Clear() {
	s.pathSegments = [32]segment{}
	s.path = nil
	s.pathCount = 0
	s.querySegments = [64]segment{}
	s.queryCount = 0
	s.unescapeCursor = 0
}

var scannerPool = &sync.Pool{
	New: func() interface{} {
		return &pathScanner{}
	},
}

type EndpointHandler struct {
	mutex             sync.RWMutex
	cacheMutex        sync.RWMutex
	apiHandlers       []*ApiNode
	staticURLHandlers map[string]*ApiNode
	responseCache     map[string]([]byte)
}

func NewEndpointHandler() *EndpointHandler {
	return &EndpointHandler{
		staticURLHandlers: make(map[string]*ApiNode),
		responseCache:     make(map[string]([]byte)),
	}
}

var paramRegExp = regexp.MustCompile(`^\{.+\}$`)

const (
	suburl = iota
	param
)

type PatternPart struct {
	Type int
	Name string
}

type QueryPart struct {
	Type  int
	Name  string
	Value string
}

var MethodAll string = "ALL"

type fieldBindingConfig struct {
	fieldIndex int
	paramName  string
	fieldType  reflect.Kind
}

type ResponseInterceptor struct {
	http.ResponseWriter
	request *http.Request
	body    *bytes.Buffer
}
type InterceptorWithFlusher struct {
	*ResponseInterceptor
}
type InterceptorWithHijacker struct {
	*ResponseInterceptor
}
type InterceptorWithFlusherAndHijacker struct {
	*ResponseInterceptor
}

func (r *ResponseInterceptor) Write(b []byte) (int, error) {
	if r.body != nil {
		r.body.Write(b)
	}

	return r.ResponseWriter.Write(b)
}

func (r *InterceptorWithFlusher) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
func (r *InterceptorWithFlusherAndHijacker) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (r *InterceptorWithHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := r.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}

	return nil, nil, nil
}
func (r *InterceptorWithFlusherAndHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := r.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}

	return nil, nil, nil
}

var cacheBufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func NewResponseInterceptor(w http.ResponseWriter, request *http.Request) (http.ResponseWriter, *ResponseInterceptor) {
	base := &ResponseInterceptor{ResponseWriter: w, request: request}

	_, isFlusher := w.(http.Flusher)
	_, isHijacker := w.(http.Hijacker)

	if isFlusher && isHijacker {
		return InterceptorWithFlusherAndHijacker{base}, base
	} else if isFlusher {
		return InterceptorWithFlusher{base}, base

	} else if isHijacker {
		return InterceptorWithHijacker{base}, base
	}

	return base, base
}

type ApiNode struct {
	Method string

	Patterns      []PatternPart
	QueryPatterns map[string]*QueryPart

	Bindings map[string]*fieldBindingConfig

	Pool        *sync.Pool
	Params      reflect.Type
	Handler     func(w http.ResponseWriter, r *http.Request, params interface{})
	cacheKeyGen func() string
}

func createSpecialPath(s string) string {
	s = strings.ReplaceAll(s, `\/`, "__SLASH__")
	s = strings.ReplaceAll(s, `\.`, "__DOT__")
	s = strings.ReplaceAll(s, `\+`, "__PLUS__")
	s = strings.ReplaceAll(s, ` `, "__SPACE__")
	s = strings.ReplaceAll(s, `\=`, "__EQUAL__")
	s = strings.ReplaceAll(s, `\&`, "__AMP__")
	s = strings.ReplaceAll(s, `\@`, "__DOG__")
	s = strings.ReplaceAll(s, `\:`, "__POINTS__")
	s = strings.ReplaceAll(s, `\?`, "__QUESTION__")
	s = strings.ReplaceAll(s, `\#`, "__ANCHOR__")
	s = strings.ReplaceAll(s, `\%`, "__PERCENT__")
	return s
}

func replaceSpecialPath(s string) string {
	s = strings.ReplaceAll(s, `__SLASH__`, "%2F")
	s = strings.ReplaceAll(s, `__DOT__`, ".")
	s = strings.ReplaceAll(s, `__PLUS__`, "%2B")
	s = strings.ReplaceAll(s, `__SPACE__`, "%20")
	s = strings.ReplaceAll(s, `__EQUAL__`, "%3D")
	s = strings.ReplaceAll(s, `__AMP__`, "%26")
	s = strings.ReplaceAll(s, `__DOG__`, "%40")
	s = strings.ReplaceAll(s, `__POINTS__`, "%3A")
	s = strings.ReplaceAll(s, `__QUESTION__`, "%3F")
	s = strings.ReplaceAll(s, `__ANCHOR__`, "%23")
	s = strings.ReplaceAll(s, `__PERCENT__`, "%25")
	return s
}

func replaceSpecailQuery(s string) string {
	s = strings.ReplaceAll(s, `__SLASH__`, "%2F")
	s = strings.ReplaceAll(s, `__SPACE__`, "+")
	s = strings.ReplaceAll(s, `__PLUS__`, "%2B")
	s = strings.ReplaceAll(s, `__EQUAL__`, "%3D")
	s = strings.ReplaceAll(s, `__AMP__`, "%26")
	s = strings.ReplaceAll(s, `__QUESTION__`, "%3F")
	s = strings.ReplaceAll(s, `__ANCHOR__`, "%23")
	s = strings.ReplaceAll(s, `__PERCENT__`, "%25")
	s = strings.ReplaceAll(s, `__DOG__`, "%40")
	s = strings.ReplaceAll(s, `__POINTS__`, "%3A")

	return s
}
func createSpecailQuery(s string) string {
	s = strings.ReplaceAll(s, `\/`, "__SLASH__")
	s = strings.ReplaceAll(s, ` `, "__SPACE__")
	s = strings.ReplaceAll(s, `\+`, "__PLUS__")
	s = strings.ReplaceAll(s, `\=`, "__EQUAL__")
	s = strings.ReplaceAll(s, `\&`, "__AMP__")
	s = strings.ReplaceAll(s, `\?`, "__QUESTION__")
	s = strings.ReplaceAll(s, `\#`, "__ANCHOR__")
	s = strings.ReplaceAll(s, `\%`, "__PERCENT__")
	s = strings.ReplaceAll(s, `\@`, "__DOG__")
	s = strings.ReplaceAll(s, `\:`, "__POINTS__")

	return s
}

func (router *EndpointHandler) HandleEndpoint(
	pattern string,
	handler func(w http.ResponseWriter, r *http.Request, params interface{}),
	method string,
	params interface{},
	cacheKeyGen func() string,
) {
	router.mutex.Lock()
	defer router.mutex.Unlock()

	urlAndQuery := strings.Split(pattern, "?")

	queryParams := ""

	if len(urlAndQuery) != 1 {
		queryParams = createSpecailQuery(urlAndQuery[1])
	}

	query, err := url.ParseQuery(queryParams)

	if err != nil {
		panic(err.Error())
	}

	pattern = createSpecialPath(urlAndQuery[0])
	splitPattern := strings.Split(urlAndQuery[0], "/")

	var patternPart []PatternPart

	var filteredSegments []string

	for _, part := range splitPattern {
		if len(part) != 0 {
			filteredSegments = append(filteredSegments, part)
		}
	}

	splitPattern = filteredSegments
	paramCount := 0

	for _, part := range splitPattern {
		isParam := paramRegExp.MatchString(part)

		if isParam {
			patternPart = append(patternPart, PatternPart{
				Type: param,
				Name: part[1 : len(part)-1],
			})

			paramCount++
		} else {
			patternPart = append(patternPart, PatternPart{
				Type: suburl,
				Name: replaceSpecialPath(url.PathEscape(part)),
			})
		}
	}

	queryPatterns := make(map[string]*QueryPart)

	for queryName, queryValue := range query {
		escapedName := replaceSpecailQuery(url.QueryEscape(createSpecailQuery(queryName)))
		part := queryValue[0]
		isParam := paramRegExp.MatchString(part)

		if isParam {
			queryPatterns[escapedName] = &QueryPart{
				Type:  param,
				Name:  escapedName,
				Value: part[1 : len(part)-1],
			}
		} else {
			value := replaceSpecailQuery(url.QueryEscape(part))

			queryPatterns[escapedName] = &QueryPart{
				Type:  suburl,
				Name:  escapedName,
				Value: value,
			}
		}

	}

	structType := reflect.TypeOf(params)
	bindings := make(map[string]*fieldBindingConfig)

	if structType != nil && structType.Kind() == reflect.Struct {
		for i := 0; i < structType.NumField(); i++ {
			field := structType.Field(i)
			tag := field.Tag.Get("routeParam")
			fieldName := ""

			if tag != "" {
				fieldName = tag
			} else {
				fieldName = field.Name
			}

			bindings[fieldName] = &fieldBindingConfig{
				fieldIndex: i,
				paramName:  fieldName,
				fieldType:  field.Type.Kind(),
			}
		}
	}

	// Пул структур
	var paramsPool *sync.Pool

	if structType != nil {
		paramsPool = &sync.Pool{
			New: func() interface{} {
				return reflect.New(structType).Interface()
			},
		}
	}

	apiNode := &ApiNode{
		Patterns:      patternPart,
		QueryPatterns: queryPatterns,
		Handler:       handler,
		Params:        structType,
		Method:        method,
		Pool:          paramsPool,
		Bindings:      bindings,
		cacheKeyGen:   cacheKeyGen,
	}

	if paramCount == 0 && len(queryPatterns) == 0 {
		router.staticURLHandlers[method+":"+replaceSpecialPath(url.PathEscape(pattern))] = apiNode
	}

	router.apiHandlers = append(router.apiHandlers, apiNode)

	slices.SortFunc(router.apiHandlers, func(a, b *ApiNode) int {
		if a.Method != b.Method {
			if a.Method == MethodAll {
				return -1
			}
			if b.Method == MethodAll {
				return 1
			}

			return cmp.Compare(a.Method, b.Method)
		}

		lenA := len(a.Patterns)
		lenB := len(b.Patterns)

		if lenA > lenB {
			return 1
		} else if lenA < lenB {
			return -1
		} else {
			for i := 0; i < lenA; i++ {
				if a.Patterns[i].Type == suburl && b.Patterns[i].Type == suburl {
					if a.Patterns[i].Name != b.Patterns[i].Name {
						return cmp.Compare(a.Patterns[i].Name, b.Patterns[i].Name)
					}

					continue
				} else if a.Patterns[i].Type == param && b.Patterns[i].Type == param {
					continue
				} else {
					if a.Patterns[i].Type == param {
						return 1
					} else {
						return -1
					}
				}
			}

			lenA = len(a.QueryPatterns)
			lenB = len(b.QueryPatterns)

			if lenA > lenB {
				return 1
			} else if lenA < lenB {
				return -1
			}

			return 0
		}
	})
}

func (router *EndpointHandler) HandleGet(
	pattern string,
	handler func(w http.ResponseWriter, r *http.Request, params interface{}),
	params interface{},
	cacheKeyGen func() string,
) {
	router.HandleEndpoint(pattern, handler, http.MethodGet, params, cacheKeyGen)
}

func (router *EndpointHandler) HandlePost(
	pattern string,
	handler func(w http.ResponseWriter, r *http.Request, params interface{}),
	params interface{},
	cacheKeyGen func() string,
) {
	router.HandleEndpoint(pattern, handler, http.MethodPost, params, cacheKeyGen)
}

func (router *EndpointHandler) HandlePut(
	pattern string,
	handler func(w http.ResponseWriter, r *http.Request, params interface{}),
	params interface{},
	cacheKeyGen func() string,
) {
	router.HandleEndpoint(pattern, handler, http.MethodPut, params, cacheKeyGen)
}

func (router *EndpointHandler) HandleDelete(
	pattern string,
	handler func(w http.ResponseWriter, r *http.Request, params interface{}),
	params interface{},
	cacheKeyGen func() string,
) {
	router.HandleEndpoint(pattern, handler, http.MethodDelete, params, cacheKeyGen)
}

func (router *EndpointHandler) HandleOptions(
	pattern string,
	handler func(w http.ResponseWriter, r *http.Request, params interface{}),
	params interface{},
	cacheKeyGen func() string,
) {
	router.HandleEndpoint(pattern, handler, http.MethodOptions, params, cacheKeyGen)
}

func (router *EndpointHandler) HandleHead(
	pattern string,
	handler func(w http.ResponseWriter, r *http.Request, params interface{}),
	params interface{},
	cacheKeyGen func() string,
) {
	router.HandleEndpoint(pattern, handler, http.MethodHead, params, cacheKeyGen)
}

func (router *EndpointHandler) HandleConnect(
	pattern string,
	handler func(w http.ResponseWriter, r *http.Request, params interface{}),
	params interface{},
	cacheKeyGen func() string,
) {
	router.HandleEndpoint(pattern, handler, http.MethodConnect, params, cacheKeyGen)
}

func (router *EndpointHandler) HandlePatch(
	pattern string,
	handler func(w http.ResponseWriter, r *http.Request, params interface{}),
	params interface{},
	cacheKeyGen func() string,
) {
	router.HandleEndpoint(pattern, handler, http.MethodPatch, params, cacheKeyGen)
}

func (router *EndpointHandler) HandleTrace(
	pattern string,
	handler func(w http.ResponseWriter, r *http.Request, params interface{}),
	params interface{},
	cacheKeyGen func() string,
) {
	router.HandleEndpoint(pattern, handler, http.MethodTrace, params, cacheKeyGen)
}

func (router *EndpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w, inter := NewResponseInterceptor(w, r)

	//io.WriteString(w, r.URL.Path)

	router.mutex.RLock()
	handlers := make([]*ApiNode, len(router.apiHandlers))
	copy(handlers, router.apiHandlers)
	router.mutex.RUnlock()

	stripPos := strings.Index(r.RequestURI, r.URL.EscapedPath())

	var path = []byte(r.RequestURI[stripPos:])

	queryPos := bytes.IndexByte(path, '?')

	if queryPos == -1 || queryPos == len(path)-1 {
		if matchedNode := router.staticURLHandlers[r.Method+":"+string(path)]; matchedNode != nil {
			cacheKey := matchedNode.cacheKeyGen()

			if matchedNode.cacheKeyGen != nil {
				router.cacheMutex.RLock()
				cache := router.responseCache[cacheKey]
				router.cacheMutex.RUnlock()

				if cache != nil {
					inter.ResponseWriter.Write(cache)

					return
				}

				inter.body = cacheBufferPool.Get().(*bytes.Buffer)
				inter.body.Reset()
			}

			matchedNode.Handler(w, r, nil)

			if inter.body != nil {
				cachedBytes := make([]byte, inter.body.Len())
				copy(cachedBytes, inter.body.Bytes())

				router.cacheMutex.Lock()
				router.responseCache[cacheKey] = cachedBytes
				router.cacheMutex.Unlock()

				cacheBufferPool.Put(inter.body)
			}

			return
		}
	}

	scanner := scannerPool.Get().(*pathScanner)

	scanner.Scan(path)

	var matchedNode *ApiNode = nil

	hStart, allStart := 0, 0
	hEnd, allEnd := 0, 0
	for i, node := range handlers {
		if node.Method == MethodAll {
			if len(node.Patterns) < scanner.pathCount {
				allStart++
			} else if len(node.Patterns) > scanner.pathCount {
				allEnd = i
			}

			if len(handlers)-1 == i {
				allEnd = len(handlers)
			}

			continue
		}

		methodCmp := cmp.Compare(node.Method, r.Method)

		if methodCmp == -1 {
			hStart++

			if len(handlers)-1 == i {
				hEnd = len(handlers)
			}
			continue
		} else if methodCmp == 1 {
			hEnd = i

			if len(handlers)-1 == i {
				hEnd = len(handlers)
			}
			break
		}

		if len(node.Patterns) < scanner.pathCount {
			hStart++

			if len(handlers)-1 == i {
				hEnd = len(handlers)
			}
			continue
		} else if len(node.Patterns) > scanner.pathCount {
			hEnd = i

			if len(handlers)-1 == i {
				hEnd = len(handlers)
			}
			break
		}

		if len(node.QueryPatterns) > scanner.queryCount {
			hEnd = i

			if len(handlers)-1 == i {
				hEnd = len(handlers)
			}
			break
		}

		if len(handlers)-1 == i {
			hEnd = len(handlers)
		}
	}

	for _, node := range handlers[hStart:hEnd] {
		isMatch := true

		for i := 0; i < scanner.pathCount; i++ {
			if node.Patterns[i].Type == suburl && node.Patterns[i].Name != scanner.Get(i) {
				isMatch = false
				break
			}
		}

		for param := range node.QueryPatterns {
			isMatchQuery := false

			for i := 0; i < scanner.queryCount; i++ {
				query := scanner.GetQuery(i)

				if query[0] == param {
					isMatchQuery = true
					break
				}
			}

			if !isMatchQuery {
				isMatch = false
				break
			}
		}

		if isMatch {
			if matchedNode != nil {
				panic(fmt.Sprintf("Нашлось более 1 обработчика для url: %s", r.URL.Path))
			} else {
				matchedNode = node
			}
		}
	}

	for _, node := range handlers[allStart:allEnd] {
		isMatch := true

		for i := 0; i < scanner.pathCount; i++ {
			if node.Patterns[i].Type == suburl && node.Patterns[i].Name != scanner.Get(i) {
				isMatch = false
				break
			}
		}

		for param := range node.QueryPatterns {
			isMatchQuery := false

			for i := 0; i < scanner.queryCount; i++ {
				query := scanner.GetQuery(i)

				if query[0] == param {
					isMatchQuery = true
					break
				}
			}

			if !isMatchQuery {
				isMatch = false
				break
			}
		}

		if isMatch {
			if matchedNode != nil {
				panic(fmt.Sprintf("Нашлось более 1 обработчика для url: %s", r.URL.Path))
			} else {
				matchedNode = node
			}
		}
	}

	if matchedNode == nil {
		scanner.Clear()
		scannerPool.Put(scanner)
		w.WriteHeader((http.StatusNotFound))
		return
	}

	if matchedNode.Params != nil {
		newStructPtr := matchedNode.Pool.Get()
		newStruct := reflect.ValueOf(newStructPtr).Elem()

		for i := 0; i < scanner.pathCount; i++ {
			if matchedNode.Patterns[i].Type == param {
				binding := matchedNode.Bindings[matchedNode.Patterns[i].Name]

				if binding != nil {
					field := newStruct.Field(binding.fieldIndex)

					//field.SetString(strings.Clone(scanner.GetUnescaped(i)))
					field.SetString(scanner.GetUnescaped(i))
				} else {
					panic(fmt.Sprintf("Биндинг роутера для параметра %s отсутствует", matchedNode.Patterns[i].Name))
				}
			}
		}

		queryIndex := 0
		for i := 0; i < scanner.queryCount; i++ {
			query := scanner.GetQueryUnescaped(i)
			queryPattern := matchedNode.QueryPatterns[query[0]]

			if queryPattern != nil {
				queryIndex++

				if queryPattern.Type == param {
					binding := matchedNode.Bindings[queryPattern.Value]

					if binding != nil {
						field := newStruct.Field(binding.fieldIndex)

						//field.SetString(strings.Clone(query[1]))
						field.SetString(query[1])
					} else {
						panic(fmt.Sprintf("Биндинг роутера для параметра %s отсутствует", queryPattern.Value))
					}
				}

				if queryIndex == len(matchedNode.QueryPatterns) {
					break
				}
			}
		}

		var cacheKey string
		if matchedNode.cacheKeyGen != nil {
			cacheKey = matchedNode.cacheKeyGen()

			router.cacheMutex.RLock()
			cache := router.responseCache[cacheKey]
			router.cacheMutex.RUnlock()

			if cache != nil {
				inter.ResponseWriter.Write(cache)

				scanner.Clear()
				scannerPool.Put(scanner)
				newStruct.Set(reflect.Zero(matchedNode.Params))
				matchedNode.Pool.Put(newStructPtr)

				return
			}

			inter.body = cacheBufferPool.Get().(*bytes.Buffer)
			inter.body.Reset()
		}

		matchedNode.Handler(w, r, newStruct.Interface())

		if inter.body != nil {
			cachedBytes := make([]byte, inter.body.Len())
			copy(cachedBytes, inter.body.Bytes())

			router.cacheMutex.Lock()
			router.responseCache[cacheKey] = cachedBytes
			router.cacheMutex.Unlock()

			cacheBufferPool.Put(inter.body)
		}

		scanner.Clear()
		scannerPool.Put(scanner)

		newStruct.Set(reflect.Zero(matchedNode.Params))
		matchedNode.Pool.Put(newStructPtr)
	} else {
		var cacheKey string
		if matchedNode.cacheKeyGen != nil {
			cacheKey = matchedNode.cacheKeyGen()

			router.cacheMutex.RLock()
			cache := router.responseCache[cacheKey]
			router.cacheMutex.RUnlock()

			if cache != nil {
				inter.ResponseWriter.Write(cache)

				scanner.Clear()
				scannerPool.Put(scanner)

				return
			}

			inter.body = cacheBufferPool.Get().(*bytes.Buffer)
			inter.body.Reset()
		}

		scanner.Clear()
		scannerPool.Put(scanner)

		matchedNode.Handler(w, r, nil)

		if inter.body != nil {
			cachedBytes := make([]byte, inter.body.Len())
			copy(cachedBytes, inter.body.Bytes())

			router.cacheMutex.Lock()
			router.responseCache[cacheKey] = cachedBytes
			router.cacheMutex.Unlock()

			cacheBufferPool.Put(inter.body)
		}
	}
}
