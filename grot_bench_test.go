package grot

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Структура параметров для замера бенчмарка
type BenchTaskParams struct {
	ProjectID string `routeParam:"ProjectID"`
	TaskID    string `routeParam:"TaskID"`
	Page      string `routeParam:"page"`
}

func BenchmarkRouter_ServeHTTP(b *testing.B) {
	// 1. Инициализируем роутер (например, с буфером на 2048 байт)
	router := NewEndpointHandler()

	// 2. Регистрируем сложный роут с параметрами пути и query-параметрами
	router.HandleGet("/projects/{ProjectID}/tasks/{TaskID}?page={page}", func(w http.ResponseWriter, r *http.Request, params interface{}) {
		// Пустой хендлер, чтобы мерить чистую скорость работы самого роутера
		w.WriteHeader(http.StatusOK)
	}, BenchTaskParams{}, func() string {
		return "s"
	})

	// 3. Создаем тестовый запрос и рекордер ответов вне цикла, чтобы не мерить их аллокации
	// Запрос содержит URL-encoded кириллицу и Query-параметры
	req := httptest.NewRequest("GET", "/projects/%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%BA%D0%B0-123/tasks/task-456?page=2", nil)
	w := httptest.NewRecorder()

	// Сбрасываем таймер перед циклом, чтобы подготовка роутера не влияла на результат
	b.ResetTimer()

	// Главный цикл бенчмарка
	for i := 0; i < b.N; i++ {
		router.ServeHTTP(w, req)
	}
}

func BenchmarkRouter_ServeHTTP_Parallel(b *testing.B) {
	router := NewEndpointHandler()

	router.HandleGet("/projects/{ProjectID}/tasks/{TaskID}?page={page}", func(w http.ResponseWriter, r *http.Request, params interface{}) {
		w.WriteHeader(http.StatusOK)
	}, BenchTaskParams{}, func() string {
		return "s"
	})

	b.ResetTimer()

	// Запуск теста параллельно на всех доступных ядрах процессора
	b.RunParallel(func(pb *testing.PB) {
		req := httptest.NewRequest("GET", "/projects/%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%BA%D0%B0-123/tasks/task-456?page=2", nil)
		w := httptest.NewRecorder()

		for pb.Next() {
			router.ServeHTTP(w, req)
		}
	})
}
