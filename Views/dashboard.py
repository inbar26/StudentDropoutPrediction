import sys
from PyQt6.QtCore import Qt, QSettings
from PyQt6.QtGui import QFont, QFontDatabase
from PyQt6.QtWidgets import QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QListWidget, QStackedWidget, QCheckBox, QComboBox

class Dashboard(QWidget):
    def __init__(self):
        super().__init__()
        self.settings = QSettings("MyApp", "Dashboard")  # Persist settings
        self.dark_mode_enabled = self.settings.value("dark_mode", False, type=bool)
        x = 5
        # Load custom font
        self.font_id = QFontDatabase.addApplicationFont("Atkinson-Hyperlegible-Mono.ttf")
        self.font_family = QFontDatabase.applicationFontFamilies(self.font_id)[0] if self.font_id != -1 else "Arial"
        
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Student Dropout Prediction Dashboard')
        self.setStyleSheet("background-color: #79bfff;")
        self.setGeometry(100, 100, 1000, 600)

        # Main Layout
        main_layout = QHBoxLayout()

        # Sidebar for Navigation
        self.sidebar = QListWidget()
        self.sidebar.addItems(["Dashboard", "Reports", "Alerts", "Student Insights"])
        self.sidebar.setFixedWidth(220)
        self.sidebar.setSpacing(5)
        self.sidebar.setStyleSheet(f'''
            QListWidget {{
                background: #2e9bff;
                color: black;
                font-family: "{self.font_family}";
                font-size: 16px;
                border-radius: 15px;
                padding: 5px;
            }}
            QListWidget::item {{
                padding: 12px;
                border-radius: 5px;
            }}
            QListWidget::item:selected {{
                background-color: #0085ff;
                color: white;
                font-weight: bold;
            }}
        ''')
        self.sidebar.currentRowChanged.connect(self.display_tab)

        # Stacked Widget for Main Content
        self.stack = QStackedWidget()
        self.stack.addWidget(self.create_dashboard_tab())
        self.stack.addWidget(self.create_reports_tab())
        self.stack.addWidget(self.create_alerts_tab())
        self.stack.addWidget(self.create_insights_tab())

        # Add to Main Layout
        main_layout.addWidget(self.sidebar)
        main_layout.addWidget(self.stack)
        
        self.setLayout(main_layout)
        self.apply_theme()

    def display_tab(self, index):
        self.stack.setCurrentIndex(index)

    def create_dashboard_tab(self):
        tab = QWidget()
        layout = QVBoxLayout()
        risk_label = QLabel('Student Risk Levels:')
        risk_label.setStyleSheet(f"font-size: 18px; font-weight: bold; color: black; font-family: '{self.font_family}';")
        layout.addWidget(risk_label)
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

    def apply_theme(self):
        self.setStyleSheet(f'''
            QWidget {{
                background-color: #79bfff;
                color: black;
                font-family: "{self.font_family}";
            }}
            QLabel, QPushButton, QListWidget {{
                color: black;
            }}
            QPushButton {{
                background-color: #f0f0f0;
                border: 1px solid #bbb;
            }}
            QPushButton:hover {{
                background-color: #e0e0e0;
            }}
        ''')

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = Dashboard()
    ex.show()
    sys.exit(app.exec())