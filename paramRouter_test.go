package paramrouter

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Структура параметров для тестирования
type TaskTestParams struct {
	ProjectID string `routeParam:"ProjectID"`
	TaskID    string `routeParam:"TaskID"`
	Page      string `routeParam:"page"`
}

func TestRouter_ServeHTTP(t *testing.T) {
	// Инициализируем наш роутер с динамическим буфером на 2048 байт
	router := NewEndpointHandler()

	// Флаги для проверки, что нужный хендлер действительно вызвался
	var staticCalled = false
	var paramsCalled = false

	// 1. Регистрируем простой статический роут
	router.HandleGet("/api/v1/status", func(w http.ResponseWriter, r *http.Request, params interface{}) {
		staticCalled = true
		w.WriteHeader(http.StatusOK)
	}, nil, func() string {
		return "s"
	})

	// 2. Регистрируем сложный роут с кириллицей, параметрами пути и обязательными Query
	router.HandleGet("/программы/{ProjectID}/задачи/{TaskID}?page={page}", func(w http.ResponseWriter, r *http.Request, params interface{}) {
		paramsCalled = true
		p := params.(TaskTestParams)

		// Проверяем, что unsafe-конвейер сканера правильно раскодировал данные и не затер их
		if p.ProjectID != "разработка-123" {
			t.Errorf("Ожидалось ProjectID 'разработка-123', получили '%s'", p.ProjectID)
		}
		if p.TaskID != "task-456" {
			t.Errorf("Ожидалось TaskID 'task-456', получили '%s'", p.TaskID)
		}
		if p.Page != "2" {
			t.Errorf("Ожидалось page '2', получили '%s'", p.Page)
		}

		w.WriteHeader(http.StatusOK)
	}, TaskTestParams{}, func() string {
		return "s2"
	})

	// Структура для описания тест-кейса (Табличный подход)
	type testCase struct {
		name           string
		method         string
		url            string
		expectedStatus int
	}

	// Описываем пограничные сценарии, включая UTF-8 и URL-Escape
	tests := []testCase{
		{
			name:           "Успешный статический роут",
			method:         "GET",
			url:            "/api/v1/status",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Ошибка 404 на несуществующий статический роут",
			method:         "GET",
			url:            "/api/v1/unknown",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Ошибка 404, если метод не совпадает (POST вместо GET)",
			method:         "POST",
			url:            "/api/v1/status",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "Успешный сложный роут с кириллицей и Query (URL-encoded)",
			method: "GET",
			// В реальности "/программы/" -> "%D0%BF%D1%80%D0%BE%D0%B3%D1%80%D0%B0%D0%BC%D0%BC%D1%8B"
			// "разработка-123" -> "%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%BA%D0%B0-123"
			// "/задачи/" -> "%D0%B7%D0%B0%D0%B4%D0%B0%D1%87%D0%B8"
			url:            "/%D0%BF%D1%80%D0%BE%D0%B3%D1%80%D0%B0%D0%BC%D0%BC%D1%8B/%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%BA%D0%B0-123/%D0%B7%D0%B0%D0%B4%D0%B0%D1%87%D0%B8/task-456?page=2",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Ошибка 404, если не передан обязательный Query параметр (?page=2)",
			method:         "GET",
			url:            "/%D0%BF%D1%80%D0%BE%D0%B3%D1%80%D0%B0%D0%BC%D0%BC%D1%8B/%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%BA%D0%B0-123/%D0%B7%D0%B0%D0%B4%D0%B0%D1%87%D0%B8/task-456",
			expectedStatus: http.StatusNotFound, // Так как query обязательный, роут без него не должен матчиться
		},
	}

	// Бежим по таблице тестов
	for _, tc := range tests {
		// t.Run создает изолированный под-тест для красивого вывода в консоли
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.url, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tc.expectedStatus {
				t.Errorf("Для кейса '%s' ожидали статус %d, но получили %d", tc.name, tc.expectedStatus, w.Code)
			}
		})
	}

	// Финальная проверка, что хендлеры вообще вызывались
	if !staticCalled {
		t.Error("Статический хендлер не был вызван")
	}
	if !paramsCalled {
		t.Error("Хендлер параметров не был вызван")
	}
}
