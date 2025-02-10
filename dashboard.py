import sys
from PyQt6.QtCore import Qt, QSettings
from PyQt6.QtWidgets import QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QListWidget, QStackedWidget, QCheckBox, QComboBox
from PyQt6.QtGui import QFont, QTranslator

class Dashboard(QWidget):
    def __init__(self):
        super().__init__()
        self.settings = QSettings("MyApp", "Dashboard")  # Persist settings
        self.dark_mode_enabled = self.settings.value("dark_mode", False, type=bool)

        self.initUI()

    def initUI(self):
        self.setWindowTitle('Student Dropout Prediction Dashboard')
        self.setGeometry(100, 100, 1000, 600)

        # Apply stored theme settings
        self.apply_theme(self.dark_mode_enabled)

        # Main Layout
        main_layout = QHBoxLayout()

        # Sidebar for Navigation
        self.sidebar = QListWidget()
        self.sidebar.addItems(["Dashboard", "Reports", "Alerts", "Student Insights", "Settings"])
        self.sidebar.setFixedWidth(220)
        self.sidebar.setSpacing(5)
        self.sidebar.setStyleSheet("""
            QListWidget::item {
                font-size: 16px;
                padding: 12px;
                border-radius: 5px;
                color: #333;
                background: #f4f4f4;
            }
            QListWidget::item:selected {
                background-color: #0078d7;
                color: #fff;
                font-weight: bold;
            }
        """)
        self.sidebar.currentRowChanged.connect(self.display_tab)

        # Stacked Widget for Main Content
        self.stack = QStackedWidget()
        self.stack.addWidget(self.create_dashboard_tab())
        self.stack.addWidget(self.create_reports_tab())
        self.stack.addWidget(self.create_alerts_tab())
        self.stack.addWidget(self.create_insights_tab())
        self.stack.addWidget(self.create_settings_tab())

        # Add to Main Layout
        main_layout.addWidget(self.sidebar)
        main_layout.addWidget(self.stack)

        self.setLayout(main_layout)

    def display_tab(self, index):
        """ Switches the displayed tab. """
        self.stack.setCurrentIndex(index)

    def create_dashboard_tab(self):
        tab = QWidget()
        layout = QVBoxLayout()
        risk_label = QLabel('Student Risk Levels:')
        risk_label.setStyleSheet("font-size: 18px; font-weight: bold; color: #333;")
        layout.addWidget(risk_label)
        layout.addWidget(QPushButton('Low'))
        layout.addWidget(QPushButton('Medium'))
        layout.addWidget(QPushButton('High'))
        tab.setLayout(layout)
        return tab

    def create_reports_tab(self):
        tab = QWidget()
        layout = QVBoxLayout()
        layout.addWidget(QLabel('Reports Overview'))
        tab.setLayout(layout)
        return tab

    def create_alerts_tab(self):
        tab = QWidget()
        layout = QVBoxLayout()
        layout.addWidget(QLabel('Alerts Monitoring'))
        tab.setLayout(layout)
        return tab

    def create_insights_tab(self):
        tab = QWidget()
        layout = QVBoxLayout()
        layout.addWidget(QLabel('Student Engagement Trends'))
        tab.setLayout(layout)
        return tab

    def create_settings_tab(self):
        """ Creates the Settings tab with theme and language options. """
        tab = QWidget()
        layout = QVBoxLayout()

        # Light/Dark Mode
        theme_label = QLabel('Theme:')
        theme_label.setStyleSheet("font-size: 16px; font-weight: bold; color: #333;")
        layout.addWidget(theme_label)

        self.theme_checkbox = QCheckBox('Dark Mode')
        self.theme_checkbox.setChecked(self.dark_mode_enabled)
        self.theme_checkbox.stateChanged.connect(self.toggle_theme)
        layout.addWidget(self.theme_checkbox)

        # Language Selection
        language_label = QLabel('Language:')
        language_label.setStyleSheet("font-size: 16px; font-weight: bold; color: #333;")
        layout.addWidget(language_label)

        self.language_combo = QComboBox()
        self.language_combo.addItems(['English', 'Hebrew'])
        self.language_combo.currentIndexChanged.connect(self.change_language)
        layout.addWidget(self.language_combo)

        tab.setLayout(layout)
        return tab

    def toggle_theme(self, state):
        """ Toggles between light and dark mode and saves preference. """
        self.dark_mode_enabled = bool(state)
        self.settings.setValue("dark_mode", self.dark_mode_enabled)
        self.apply_theme(self.dark_mode_enabled)

    def apply_theme(self, dark_mode):
        """ Applies the selected theme. """
        if dark_mode:
            self.setStyleSheet("""
                QWidget {
                    background-color: #2b2b2b;
                    color: #fff;
                    font-family: 'Segoe UI', Arial, sans-serif;
                }
                QLabel, QPushButton, QListWidget {
                    color: #fff;
                }
                QPushButton {
                    background-color: #444;
                    border: 1px solid #666;
                }
                QPushButton:hover {
                    background-color: #555;
                }
                QListWidget::item {
                    background: #333;
                    color: #bbb;
                }
                QListWidget::item:selected {
                    background-color: #555;
                    color: #fff;
                }
                QStackedWidget {
                    background-color: #333;
                }
                QCheckBox, QComboBox {
                    background-color: #444;
                    color: #fff;
                    border: 1px solid #666;
                }
            """)
        else:
            self.setStyleSheet("""
                QWidget {
                    background-color: #f9f9f9;
                    color: #333;
                    font-family: 'Segoe UI', Arial, sans-serif;
                }
                QLabel, QPushButton, QListWidget {
                    color: #333;
                }
                QPushButton {
                    background-color: #f0f0f0;
                    border: 1px solid #bbb;
                }
                QPushButton:hover {
                    background-color: #e0e0e0;
                }
                QListWidget::item {
                    background: #f4f4f4;
                    color: #333;
                }
                QListWidget::item:selected {
                    background-color: #0078d7;
                    color: #fff;
                    font-weight: bold;
                }
                QStackedWidget {
                    background-color: #fff;
                }
                QCheckBox, QComboBox {
                    background-color: #fff;
                    color: #333;
                    border: 1px solid #ccc;
                }
            """)

    def change_language(self, index):
        """ Handles language selection change. """
        selected_language = self.language_combo.itemText(index)
        translator = QTranslator()
        if selected_language == "Hebrew":
            # Load Hebrew translation file if available
            pass  # Add actual translation implementation here
        else:
            pass  # Default to English

        print(f"Language changed to: {selected_language}")  # Placeholder for actual implementation


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = Dashboard()
    ex.show()
    sys.exit(app.exec())
