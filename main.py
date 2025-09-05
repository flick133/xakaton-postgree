import sys
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QLabel, QLineEdit, QTextEdit, QPushButton,
                             QGroupBox, QMessageBox, QFormLayout, QTabWidget, QTableWidget,
                             QTableWidgetItem, QHeaderView, QSplitter, QCheckBox)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont, QColor
import psycopg2
from psycopg2 import OperationalError
import json


class DatabaseApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Database Query Tool with EXPLAIN Analysis')
        self.setGeometry(100, 100, 900, 700)

        # Центральный виджет
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # Основной layout
        main_layout = QVBoxLayout(central_widget)

        # Создаем вкладки
        self.tabs = QTabWidget()

        # Вкладка подключения и запроса
        query_tab = QWidget()
        query_layout = QVBoxLayout(query_tab)

        # Группа для настроек подключения
        connection_group = QGroupBox("Настройки подключения к базе данных")
        connection_layout = QFormLayout()

        # Поля для ввода данных
        self.host_input = QLineEdit()
        self.host_input.setPlaceholderText("например, localhost или 192.168.1.100")

        self.port_input = QLineEdit()
        self.port_input.setPlaceholderText("5432")
        self.port_input.setText("5432")

        self.database_input = QLineEdit()
        self.database_input.setPlaceholderText("имя базы данных")

        self.user_input = QLineEdit()
        self.user_input.setPlaceholderText("пользователь")

        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("пароль")
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)

        # Добавляем поля в форму
        connection_layout.addRow("Хост (IP):", self.host_input)
        connection_layout.addRow("Порт:", self.port_input)
        connection_layout.addRow("База данных:", self.database_input)
        connection_layout.addRow("Пользователь:", self.user_input)
        connection_layout.addRow("Пароль:", self.password_input)

        connection_group.setLayout(connection_layout)

        # Группа для SQL запроса
        sql_group = QGroupBox("SQL Запрос")
        sql_layout = QVBoxLayout()

        self.sql_input = QTextEdit()
        self.sql_input.setPlaceholderText("Введите ваш SQL запрос здесь...\nПример: SELECT * FROM aircrafts_data;")
        self.sql_input.setMinimumHeight(100)

        # Опции для EXPLAIN
        options_layout = QHBoxLayout()
        self.analyze_checkbox = QCheckBox("ANALYZE (выполнить запрос)")
        self.analyze_checkbox.setToolTip("Фактически выполнит запрос для получения точных метрик")
        self.verbose_checkbox = QCheckBox("VERBOSE")
        self.buffers_checkbox = QCheckBox("BUFFERS")

        options_layout.addWidget(self.analyze_checkbox)
        options_layout.addWidget(self.verbose_checkbox)
        options_layout.addWidget(self.buffers_checkbox)
        options_layout.addStretch()

        sql_layout.addWidget(self.sql_input)
        sql_layout.addLayout(options_layout)
        sql_group.setLayout(sql_layout)

        # Кнопки
        buttons_layout = QHBoxLayout()

        self.explain_button = QPushButton("Анализировать план (EXPLAIN)")
        self.explain_button.clicked.connect(self.analyze_query_plan)
        self.explain_button.setStyleSheet("QPushButton { background-color: #4CAF50; color: white; }")

        self.execute_button = QPushButton("Выполнить запрос")
        self.execute_button.clicked.connect(self.execute_query)

        self.clear_button = QPushButton("Очистить")
        self.clear_button.clicked.connect(self.clear_fields)

        buttons_layout.addWidget(self.explain_button)
        buttons_layout.addWidget(self.execute_button)
        buttons_layout.addWidget(self.clear_button)

        query_layout.addWidget(connection_group)
        query_layout.addWidget(sql_group)
        query_layout.addLayout(buttons_layout)

        # Вкладка результатов
        result_tab = QWidget()
        result_layout = QVBoxLayout(result_tab)

        # Сплиттер для разделения результатов и плана выполнения
        splitter = QSplitter(Qt.Orientation.Vertical)

        # Группа для результатов запроса
        self.result_group = QGroupBox("Результаты запроса")
        result_group_layout = QVBoxLayout()

        self.result_table = QTableWidget()
        self.result_table.setAlternatingRowColors(True)
        result_group_layout.addWidget(self.result_table)
        self.result_group.setLayout(result_group_layout)

        # Группа для анализа плана выполнения
        self.plan_group = QGroupBox("Анализ плана выполнения")
        plan_layout = QVBoxLayout()

        self.plan_text = QTextEdit()
        self.plan_text.setReadOnly(True)
        self.plan_text.setFont(QFont("Courier", 10))
        self.plan_text.setMinimumHeight(200)

        # Таблица с метриками
        self.metrics_table = QTableWidget()
        self.metrics_table.setColumnCount(2)
        self.metrics_table.setHorizontalHeaderLabels(["Метрика", "Значение"])
        self.metrics_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.metrics_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.metrics_table.setMaximumHeight(150)

        plan_layout.addWidget(QLabel("Детальный план:"))
        plan_layout.addWidget(self.plan_text)
        plan_layout.addWidget(QLabel("Ключевые метрики:"))
        plan_layout.addWidget(self.metrics_table)

        self.plan_group.setLayout(plan_layout)

        splitter.addWidget(self.result_group)
        splitter.addWidget(self.plan_group)
        splitter.setSizes([300, 400])

        result_layout.addWidget(splitter)

        # Добавляем вкладки
        self.tabs.addTab(query_tab, "Запрос")
        self.tabs.addTab(result_tab, "Результаты")

        main_layout.addWidget(self.tabs)

    def get_connection(self):
        """Создает и возвращает соединение с базой данных"""
        host = self.host_input.text().strip()
        port = self.port_input.text().strip()
        database = self.database_input.text().strip()
        user = self.user_input.text().strip()
        password = self.password_input.text().strip()

        return psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

    def analyze_query_plan(self):
        """Анализирует план выполнения запроса"""
        sql_query = self.sql_input.toPlainText().strip()

        if not sql_query:
            QMessageBox.warning(self, "Ошибка", "Пожалуйста, введите SQL запрос!")
            return

        try:
            connection = self.get_connection()
            cursor = connection.cursor()

            # Формируем EXPLAIN запрос
            explain_options = []
            if self.analyze_checkbox.isChecked():
                explain_options.append("ANALYZE")
            if self.verbose_checkbox.isChecked():
                explain_options.append("VERBOSE")
            if self.buffers_checkbox.isChecked():
                explain_options.append("BUFFERS")

            explain_query = f"EXPLAIN ({', '.join(explain_options)}) {sql_query}"

            cursor.execute(explain_query)
            plan_result = cursor.fetchall()

            # Форматируем план выполнения
            plan_text = "\n".join([row[0] for row in plan_result])
            self.plan_text.setPlainText(plan_text)

            # Анализируем метрики из плана
            self.analyze_plan_metrics(plan_text)

            # Переключаемся на вкладку результатов
            self.tabs.setCurrentIndex(1)

            cursor.close()
            connection.close()

        except OperationalError as e:
            QMessageBox.critical(self, "Ошибка подключения", f"Ошибка подключения к базе данных:\n{str(e)}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка анализа", f"Ошибка анализа плана выполнения:\n{str(e)}")

    def analyze_plan_metrics(self, plan_text):
        """Анализирует метрики из плана выполнения"""
        metrics = {}

        # Извлекаем общую стоимость
        if "cost=" in plan_text:
            cost_match = plan_text.split("cost=")[1].split("..")[1].split(" ")[0]
            metrics["Общая стоимость"] = f"{float(cost_match):.2f}"

        # Извлекаем время выполнения
        if "actual time=" in plan_text:
            time_match = plan_text.split("actual time=")[1].split("..")[1].split(" ")[0]
            metrics["Время выполнения (мс)"] = f"{float(time_match):.2f}"

        # Извлекаем количество строк
        if "rows=" in plan_text:
            rows_match = plan_text.split("rows=")[1].split(" ")[0]
            metrics["Ожидаемое количество строк"] = rows_match

        # Извлекаем использование памяти
        if "Buffers:" in plan_text:
            buffers_text = plan_text.split("Buffers:")[1].split("\n")[0]
            metrics["Использование буферов"] = buffers_text.strip()

        # Заполняем таблицу метрик
        self.metrics_table.setRowCount(len(metrics))
        for i, (key, value) in enumerate(metrics.items()):
            self.metrics_table.setItem(i, 0, QTableWidgetItem(key))
            self.metrics_table.setItem(i, 1, QTableWidgetItem(value))

    def execute_query(self):
        """Выполняет SQL запрос"""
        sql_query = self.sql_input.toPlainText().strip()

        if not sql_query:
            QMessageBox.warning(self, "Ошибка", "Пожалуйста, введите SQL запрос!")
            return

        try:
            connection = self.get_connection()
            cursor = connection.cursor()

            cursor.execute(sql_query)

            # Если запрос возвращает данные
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                # Заполняем таблицу результатов
                self.result_table.setColumnCount(len(columns))
                self.result_table.setRowCount(len(rows))
                self.result_table.setHorizontalHeaderLabels(columns)

                for row_idx, row in enumerate(rows):
                    for col_idx, value in enumerate(row):
                        item = QTableWidgetItem(str(value) if value is not None else "NULL")
                        self.result_table.setItem(row_idx, col_idx, item)

                # Автоподбор размера колонок
                self.result_table.resizeColumnsToContents()

            else:
                # Для запросов без возвращаемых данных
                connection.commit()
                self.result_table.setRowCount(0)
                self.result_table.setColumnCount(0)
                self.plan_text.setPlainText(f"Запрос выполнен успешно! Затронуто строк: {cursor.rowcount}")

            # Показываем вкладку результатов
            self.tabs.setCurrentIndex(1)

            cursor.close()
            connection.close()

        except OperationalError as e:
            QMessageBox.critical(self, "Ошибка подключения", f"Ошибка подключения к базе данных:\n{str(e)}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка выполнения", f"Ошибка выполнения запроса:\n{str(e)}")

    def clear_fields(self):
        """Очищает все поля"""
        self.host_input.clear()
        self.port_input.clear()
        self.database_input.clear()
        self.user_input.clear()
        self.password_input.clear()
        self.sql_input.clear()
        self.result_table.setRowCount(0)
        self.result_table.setColumnCount(0)
        self.plan_text.clear()
        self.metrics_table.setRowCount(0)


def main():
    app = QApplication(sys.argv)
    window = DatabaseApp()
    window.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()