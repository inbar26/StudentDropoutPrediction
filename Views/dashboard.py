import sys
import json
import pandas as pd
from confluent_kafka import Producer, Consumer
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont, QFontDatabase
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QListWidget, QStackedWidget, QLineEdit, QFileDialog, QComboBox, QMessageBox, QRadioButton, QScrollArea, QFileDialog, QButtonGroup
)

from Models.model import check_for_new_labeled_data, predict_new_student_data



producer = Producer({'bootstrap.servers': 'localhost:9092'})


class Dashboard(QWidget):
    def __init__(self):
        super().__init__()

        with open("/Users/rubinroy/Desktop/BigDataFinalProject/StudentDropoutPrediction/Views/styles.css", "r") as f:
            self.styles = f.read()
            self.setStyleSheet(self.styles)

        self.font_id = QFontDatabase.addApplicationFont("Atkinson-Hyperlegible-Mono.ttf")
        self.font_family = QFontDatabase.applicationFontFamilies(self.font_id)[0] if self.font_id != -1 else "Arial"
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Student Dropout Prediction Dashboard')
        self.setStyleSheet("background-color: #79bfff;")
        self.setGeometry(100, 100, 1000, 700)

        main_layout = QHBoxLayout()

        self.sidebar = QListWidget()
        self.sidebar.addItems(["Student Insights", "Add Students Data"])
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

        self.stack = QStackedWidget()
        self.stack.addWidget(self.create_insights_tab())
        self.stack.addWidget(self.create_data_tab())

        main_layout.addWidget(self.sidebar)
        main_layout.addWidget(self.stack)
        self.setLayout(main_layout)
        self.apply_theme()

    def display_tab(self, index):
        self.stack.setCurrentIndex(index)

    def create_insights_tab(self):
        tab = QWidget()
        layout = QVBoxLayout()

        layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        self.id_input = QLineEdit()
        self.id_input.setPlaceholderText("Enter Student ID")
        layout.addWidget(self.id_input)

        check_button = QPushButton("🔍 Check Dropout Prediction")

        check_button.clicked.connect(self.check_student_prediction)
        layout.addWidget(check_button)

        self.insight_result_label = QLabel("")
        self.insight_result_label.setWordWrap(True)
        layout.addWidget(self.insight_result_label)

        tab.setLayout(layout)
        return tab


    def create_data_tab(self):
        tab = QWidget()
        layout = QVBoxLayout()

        layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setContentsMargins(20, 40, 40, 40)
        layout.setSpacing(15)

       # Selection screen
        self.selection_widget = QWidget()
        selection_layout = QVBoxLayout()

        button_row = QHBoxLayout()
        button_row.setSpacing(20)
        button_row.setAlignment(Qt.AlignmentFlag.AlignCenter)

        upload_csv_btn = QPushButton("📄 Upload CSV File")
        upload_csv_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        upload_csv_btn.setFixedSize(180, 40)
        upload_csv_btn.clicked.connect(lambda: self.data_tab_stack.setCurrentWidget(self.csv_upload_widget))
        button_row.addWidget(upload_csv_btn)

        add_manual_btn = QPushButton("📝 Add Manual Data")
        add_manual_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        add_manual_btn.setFixedSize(180, 40)
        add_manual_btn.clicked.connect(lambda: self.data_tab_stack.setCurrentWidget(self.manual_scroll))
        button_row.addWidget(add_manual_btn)

        selection_layout.addLayout(button_row)
        self.selection_widget.setLayout(selection_layout)

        # CSV Upload screen
        self.csv_upload_widget = QWidget()
        csv_layout = QVBoxLayout()

        self.csv_back_btn = QPushButton("← Back")
        self.csv_back_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.csv_back_btn.setFixedSize(180, 40)
        self.csv_back_btn.clicked.connect(self.handle_csv_back)
        csv_layout.addWidget(self.csv_back_btn, alignment=Qt.AlignmentFlag.AlignLeft)

        self.upload_btn = QPushButton("Upload CSV File")
        self.upload_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.upload_btn.setFixedSize(180, 40)
        self.upload_btn.clicked.connect(self.upload_csv)
        csv_layout.addWidget(self.upload_btn, alignment=Qt.AlignmentFlag.AlignCenter)


        self.csv_status_label = QLabel("")
        csv_layout.addWidget(self.csv_status_label)

        self.csv_upload_widget.setLayout(csv_layout)

        # Manual Form widget (placeholder for now)
        self.manual_form_widget = QWidget()
        manual_layout = QVBoxLayout()

        self.manual_back_btn = QPushButton("← Back")
        self.manual_back_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.manual_back_btn.clicked.connect(lambda: (self.reset_manual_form(), self.data_tab_stack.setCurrentWidget(self.selection_widget)))        
        manual_layout.addWidget(self.manual_back_btn)

        self.manual_form_widget.setLayout(manual_layout)

        
        self.student_id_input = QLineEdit()
        self.student_id_input.setPlaceholderText("ID")
        manual_layout.addWidget(self.student_id_input)

        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("First Name")
        manual_layout.addWidget(self.name_input)

        self.last_name_input = QLineEdit()
        self.last_name_input.setPlaceholderText("Last Name")
        manual_layout.addWidget(self.last_name_input)

        self.martial_input = QComboBox()
        self.martial_input.addItem("Select Marital Status")
        self.martial_input.addItems(["Single", "Married", "Widower", "Divorced", "Common-law marriage", "Legally separated"])
        manual_layout.addWidget(self.martial_input)

        self.application_mode_input = QComboBox()
        self.application_mode_input.addItem("Select Application Mode")
        self.application_mode_input.addItems([
            "1st phase—general contingent",
            "Ordinance No. 612/93",
            "1st phase—special contingent (Azores Island)",
            "Holders of other higher courses",
            "Ordinance No. 854-B/99",
            "International student (bachelor)",
            "1st phase—special contingent (Madeira Island)",
            "2nd phase—general contingent",
            "3rd phase—general contingent",
            "Ordinance No. 533-A/99, item b2) (Different Plan)",
            "Ordinance No. 533-A/99, item b3 (Other Institution)",
            "Over 23 years old",
            "13Transfer",
            "Change in course",
            "Technological specialization diploma holders",
            "Change in institution/course",
            "Short cycle diploma holders",
            "Change in institution/course (International)"
        ])
        manual_layout.addWidget(self.application_mode_input)

        self.application_order_input = QLineEdit()
        self.application_order_input.setPlaceholderText("application_order")
        manual_layout.addWidget(self.application_order_input)

        self.course_input = QComboBox()
        self.course_input.addItem("Select Course")
        self.course_input.addItems([
            "Biofuel Production Technologies",
            "Animation and Multimedia Design",
            "Social Service (evening attendance)",
            "Agronomy",
            "Communication Design",
            "Veterinary Nursing",
            "Informatics Engineering",
            "Equiniculture",
            "Management",
            "Social Service",
            "Tourism",
            "Nursing",
            "Oral Hygiene",
            "Advertising and Marketing Management",
            "Journalism and Communication",
            "Basic Education",
            "Management (evening attendance)"
        ])
        manual_layout.addWidget(self.course_input)

        self.attendance_input = QComboBox()
        self.attendance_input.addItem("Select Attendance")
        self.attendance_input.addItems(["Evening", "Daytime"])
        manual_layout.addWidget(self.attendance_input)

        self.previous_qualification_input = QComboBox()
        self.previous_qualification_input.addItem("Select Previous Qualification")
        self.previous_qualification_input.addItems([
            "Secondary education",
            "Higher education—bachelor's degree",
            "Higher education—degree",
            "Higher education—master's degree",
            "Higher education—doctorate",
            "Frequency of higher education",
            "12th year of schooling—not completed",
            "11th year of schooling—not completed",
            "Other—11th year of schooling",
            "10th year of schooling",
            "10th year of schooling—not completed",
            "Basic education 3rd cycle (9th/10th/11th year) or equivalent",
            "Basic education 2nd cycle (6th/7th/8th year) or equivalent",
            "Technological specialization course",
            "Higher education—degree (1st cycle)",
            "Professional higher technical course",
            "Higher education—master's degree (2nd cycle)"
        ])
        manual_layout.addWidget(self.previous_qualification_input)

        self.mothers_qualification_input = QComboBox()
        self.mothers_qualification_input.addItem("Select Mother's Qualification")
        self.mothers_qualification_input.addItems([
            "Secondary Education—12th Year of Schooling or Equivalent",
            "Higher Education—bachelor's degree",
            "Higher Education—degree",
            "Higher Education—master's degree",
            "Higher Education—doctorate",
            "Frequency of Higher Education",
            "12th Year of Schooling—not completed",
            "11th Year of Schooling—not completed",
            "7th Year (Old)",
            "Other—11th Year of Schooling",
            "2nd year complementary high school course",
            "10th Year of Schooling",
            "General commerce course",
            "Basic Education 3rd Cycle (9th/10th/11th Year) or Equivalent",
            "Complementary High School Course",
            "Technical-professional course",
            "Complementary High School Course—not concluded",
            "7th year of schooling",
            "2nd cycle of the general high school course",
            "9th Year of Schooling—not completed",
            "8th year of schooling",
            "General Course of Administration and Commerce",
            "Supplementary Accounting and Administration",
            "Unknown",
            "Cannot read or write",
            "Can read without having a 4th year of schooling",
            "Basic education 1st cycle (4th/5th year) or equivalent",
            "Basic Education 2nd Cycle (6th/7th/8th Year) or equivalent",
            "Technological specialization course",
            "Higher education—degree (1st cycle)",
            "Specialized higher studies course",
            "Professional higher technical course",
            "Higher Education—master's degree (2nd cycle)",
            "Higher Education—doctorate (3rd cycle)"
        ])
        manual_layout.addWidget(self.mothers_qualification_input)

        self.fathers_qualification_input = QComboBox()
        self.fathers_qualification_input.addItem("Select Father's Qualification")
        self.fathers_qualification_input.addItems([
            "Secondary Education—12th Year of Schooling or Equivalent",
            "Higher Education—bachelor's degree",
            "Higher Education—degree",
            "Higher Education—master's degree",
            "Higher Education—doctorate",
            "Frequency of Higher Education",
            "12th Year of Schooling—not completed",
            "11th Year of Schooling—not completed",
            "7th Year (Old)",
            "Other—11th Year of Schooling",
            "2nd year complementary high school course",
            "10th Year of Schooling",
            "General commerce course",
            "Basic Education 3rd Cycle (9th/10th/11th Year) or Equivalent",
            "Complementary High School Course",
            "Technical-professional course",
            "Complementary High School Course—not concluded",
            "7th year of schooling",
            "2nd cycle of the general high school course",
            "9th Year of Schooling—not completed",
            "8th year of schooling",
            "General Course of Administration and Commerce",
            "Supplementary Accounting and Administration",
            "Unknown",
            "Cannot read or write",
            "Can read without having a 4th year of schooling",
            "Basic education 1st cycle (4th/5th year) or equivalent",
            "Basic Education 2nd Cycle (6th/7th/8th Year) or equivalent",
            "Technological specialization course",
            "Higher education—degree (1st cycle)",
            "Specialized higher studies course",
            "Professional higher technical course",
            "Higher Education—master's degree (2nd cycle)",
            "Higher Education—doctorate (3rd cycle)"
        ])
        manual_layout.addWidget(self.fathers_qualification_input)

        self.mothers_occuption_input = QComboBox()
        self.mothers_occuption_input.addItem("Select Mother's Occupation")
        self.mothers_occuption_input.addItems([
            "Student",
            "Representatives of the Legislative Power and Executive Bodies, Directors, Directors and Executive Managers",
            "Specialists in Intellectual and Scientific Activities",
            "Intermediate Level Technicians and Professions",
            "Administrative staff",
            "Personal Services, Security and Safety Workers, and Sellers",
            "Farmers and Skilled Workers in Agriculture, Fisheries, and Forestry",
            "Skilled Workers in Industry, Construction, and Craftsmen",
            "Installation and Machine Operators and Assembly Workers",
            "Unskilled Workers",
            "Armed Forces Professions",
            "Other Situation; 13—(blank)",
            "Armed Forces Officers",
            "Armed Forces Sergeants",
            "Other Armed Forces personnel",
            "Directors of administrative and commercial services",
            "Hotel, catering, trade, and other services directors",
            "Specialists in the physical sciences, mathematics, engineering, and related techniques",
            "Health professionals",
            "Teachers",
            "Specialists in finance, accounting, administrative organization, and public and commercial relations",
            "Intermediate level science and engineering technicians and professions",
            "Technicians and professionals of intermediate level of health",
            "Intermediate level technicians from legal, social, sports, cultural, and similar services",
            "Information and communication technology technicians",
            "Office workers, secretaries in general, and data processing operators",
            "Data, accounting, statistical, financial services, and registry-related operators",
            "Other administrative support staff",
            "Personal service workers",
            "Sellers",
            "Personal care workers and the like",
            "Protection and security services personnel",
            "Market-oriented farmers and skilled agricultural and animal production workers",
            "Farmers, livestock keepers, fishermen, hunters and gatherers, and subsistence",
            "Skilled construction workers and the like, except electricians",
            "Skilled workers in metallurgy, metalworking, and similar",
            "Skilled workers in electricity and electronics",
            "Workers in food processing, woodworking, and clothing and other industries and crafts",
            "Fixed plant and machine operators",
            "Assembly workers",
            "Vehicle drivers and mobile equipment operators",
            "Unskilled workers in agriculture, animal production, and fisheries and forestry",
            "Unskilled workers in extractive industry, construction, manufacturing, and transport",
            "Meal preparation assistants",
            "Street vendors (except food) and street service provider"
        ])
        manual_layout.addWidget(self.mothers_occuption_input)

        self.fathers_occuption_input = QComboBox()
        self.fathers_occuption_input.addItem("Select Father's Occupation")
        self.fathers_occuption_input.addItems([
            "Student",
            "Representatives of the Legislative Power and Executive Bodies, Directors, Directors and Executive Managers",
            "Specialists in Intellectual and Scientific Activities",
            "Intermediate Level Technicians and Professions",
            "Administrative staff",
            "Personal Services, Security and Safety Workers, and Sellers",
            "Farmers and Skilled Workers in Agriculture, Fisheries, and Forestry",
            "Skilled Workers in Industry, Construction, and Craftsmen",
            "Installation and Machine Operators and Assembly Workers",
            "Unskilled Workers",
            "Armed Forces Professions",
            "Other Situation; 13—(blank)",
            "Armed Forces Officers",
            "Armed Forces Sergeants",
            "Other Armed Forces personnel",
            "Directors of administrative and commercial services",
            "Hotel, catering, trade, and other services directors",
            "Specialists in the physical sciences, mathematics, engineering, and related techniques",
            "Health professionals",
            "Teachers",
            "Specialists in finance, accounting, administrative organization, and public and commercial relations",
            "Intermediate level science and engineering technicians and professions",
            "Technicians and professionals of intermediate level of health",
            "Intermediate level technicians from legal, social, sports, cultural, and similar services",
            "Information and communication technology technicians",
            "Office workers, secretaries in general, and data processing operators",
            "Data, accounting, statistical, financial services, and registry-related operators",
            "Other administrative support staff",
            "Personal service workers",
            "Sellers",
            "Personal care workers and the like",
            "Protection and security services personnel",
            "Market-oriented farmers and skilled agricultural and animal production workers",
            "Farmers, livestock keepers, fishermen, hunters and gatherers, and subsistence",
            "Skilled construction workers and the like, except electricians",
            "Skilled workers in metallurgy, metalworking, and similar",
            "Skilled workers in electricity and electronics",
            "Workers in food processing, woodworking, and clothing and other industries and crafts",
            "Fixed plant and machine operators",
            "Assembly workers",
            "Vehicle drivers and mobile equipment operators",
            "Unskilled workers in agriculture, animal production, and fisheries and forestry",
            "Unskilled workers in extractive industry, construction, manufacturing, and transport",
            "Meal preparation assistants",
            "Street vendors (except food) and street service provider"
        ])
        manual_layout.addWidget(self.fathers_occuption_input)

        self.displaced_input = QComboBox()
        self.displaced_input.addItem("Select if Displaced")
        self.displaced_input.addItems(["Yes", "No"])
        manual_layout.addWidget(self.displaced_input)

        self.debtor_input = QComboBox()
        self.debtor_input.addItem("Debtor")
        self.debtor_input.addItems(["Yes", "No"])
        manual_layout.addWidget(self.debtor_input)

        self.tuition_fees_up_to_date_input = QComboBox()
        self.tuition_fees_up_to_date_input.addItem("Tuition Fees Up to Date")
        self.tuition_fees_up_to_date_input.addItems(["Yes", "No"])
        manual_layout.addWidget(self.tuition_fees_up_to_date_input)

        self.scholarship_input = QComboBox()
        self.scholarship_input.addItem("Scholarship")
        self.scholarship_input.addItems(["Yes", "No"])
        manual_layout.addWidget(self.scholarship_input)

        self.age_at_enrollment_input = QLineEdit()
        self.age_at_enrollment_input.setPlaceholderText("Age at Enrollment")
        manual_layout.addWidget(self.age_at_enrollment_input)

        self.gender_input = QComboBox()
        self.gender_input.addItem("Select Gender")
        self.gender_input.addItems(["Female", "Male"])
        manual_layout.addWidget(self.gender_input)

        self.Curricular_units_1st_sem_credited_input = QLineEdit()
        self.Curricular_units_1st_sem_credited_input.setPlaceholderText("Curricular Units 1st Sem (credited)")
        manual_layout.addWidget(self.Curricular_units_1st_sem_credited_input)

        self.Curricular_units_1st_sem_enrolled_input = QLineEdit()
        self.Curricular_units_1st_sem_enrolled_input.setPlaceholderText("Curricular Units 1st Sem (enrolled)")
        manual_layout.addWidget(self.Curricular_units_1st_sem_enrolled_input)

        self.Curricular_units_1st_sem_evaluations_input = QLineEdit()
        self.Curricular_units_1st_sem_evaluations_input.setPlaceholderText("Curricular Units 1st Sem (evaluations)")
        manual_layout.addWidget(self.Curricular_units_1st_sem_evaluations_input)

        self.Curricular_units_1st_sem_approved_input = QLineEdit()
        self.Curricular_units_1st_sem_approved_input.setPlaceholderText("Curricular Units 1st Sem (approved)")
        manual_layout.addWidget(self.Curricular_units_1st_sem_approved_input)

        self.Curricular_units_1st_sem_grade_input = QLineEdit()
        self.Curricular_units_1st_sem_grade_input.setPlaceholderText("Curricular Units 1st Sem (grade)")
        manual_layout.addWidget(self.Curricular_units_1st_sem_grade_input)

        self.Curricular_units_1st_sem_without_evealuations_input = QLineEdit()
        self.Curricular_units_1st_sem_without_evealuations_input.setPlaceholderText("Curricular Units 1st Sem (without Evaluations)")
        manual_layout.addWidget(self.Curricular_units_1st_sem_without_evealuations_input)

        self.Curricular_units_2nd_sem_credited_input = QLineEdit()
        self.Curricular_units_2nd_sem_credited_input.setPlaceholderText("Curricular Units 2nd Sem (credited)")
        manual_layout.addWidget(self.Curricular_units_2nd_sem_credited_input)
        
        self.Curricular_units_2nd_sem_enrolled_input = QLineEdit()
        self.Curricular_units_2nd_sem_enrolled_input.setPlaceholderText("Curricular Units 2nd Sem (enrolled)")
        manual_layout.addWidget(self.Curricular_units_2nd_sem_enrolled_input)

        self.Curricular_units_2nd_sem_evaluations_input = QLineEdit()
        self.Curricular_units_2nd_sem_evaluations_input.setPlaceholderText("Curricular Units 2nd Sem (evaluations)")
        manual_layout.addWidget(self.Curricular_units_2nd_sem_evaluations_input)
        
        self.Curricular_units_2nd_sem_approved_input = QLineEdit()
        self.Curricular_units_2nd_sem_approved_input.setPlaceholderText("Curricular Units 2nd Sem (approved)")
        manual_layout.addWidget(self.Curricular_units_2nd_sem_approved_input)

        self.Curricular_units_2nd_sem_grade_input = QLineEdit()
        self.Curricular_units_2nd_sem_grade_input.setPlaceholderText("Curricular Units 2nd Sem (grade)")
        manual_layout.addWidget(self.Curricular_units_2nd_sem_grade_input)

        self.Curricular_units_2nd_sem_without_evealuations_input = QLineEdit()
        self.Curricular_units_2nd_sem_without_evealuations_input.setPlaceholderText("Curricular Units 2nd Sem (without Evaluations)")
        manual_layout.addWidget(self.Curricular_units_2nd_sem_without_evealuations_input)

        self.unemployment_rate_input = QLineEdit()
        self.unemployment_rate_input.setPlaceholderText("Unemployment Rate")
        manual_layout.addWidget(self.unemployment_rate_input)

        self.inflation_rate_input = QLineEdit()
        self.inflation_rate_input.setPlaceholderText("Inflation Rate")
        manual_layout.addWidget(self.inflation_rate_input)

        self.gpa_input = QLineEdit()
        self.gpa_input.setPlaceholderText("GPA")
        manual_layout.addWidget(self.gpa_input)

        target_label = QLabel("Is this student graduated or dropouted? (Optional)")
        manual_layout.addWidget(target_label)

        self.target_yes_radio = QRadioButton("Graduated")
        self.target_no_radio = QRadioButton("Dropouted")
        target_radio_layout = QHBoxLayout()
        target_radio_layout.addWidget(self.target_yes_radio)
        target_radio_layout.addWidget(self.target_no_radio)
        manual_layout.addLayout(target_radio_layout)

        send_btn = QPushButton("Upload Studant Data")
        send_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        send_btn.clicked.connect(self.send_manual_data)
        manual_layout.addWidget(send_btn)
        
        self.manual_form_widget.setLayout(manual_layout)

        self.manual_scroll = QScrollArea()
        self.manual_scroll.setWidgetResizable(True)
        self.manual_scroll.setWidget(self.manual_form_widget)
        

        # Stack to switch between views
        self.data_tab_stack = QStackedWidget()
        self.data_tab_stack.addWidget(self.selection_widget)
        self.data_tab_stack.addWidget(self.csv_upload_widget)
        self.data_tab_stack.addWidget(self.manual_scroll)
        
        layout.addWidget(self.data_tab_stack)
        tab.setLayout(layout)

        return tab

    def send_manual_data(self):
        try:

            target = None
            if self.target_yes_radio.isChecked():
                target = "Graduate"
            elif self.target_no_radio.isChecked():
                target = "Dropout"

            student_data = {
                  "Student ID": str(self.student_id_input.text().strip()),
                  "First Name": str(self.name_input.text().strip()),
                  "Last Name": str(self.last_name_input.text().strip()),
                  "Marital status": str(self.martial_input.currentIndex()),
                  "Application mode": str(self.application_mode_input.currentIndex()),
                  "Application order": str(self.application_order_input.text()),
                  "Course": str(self.course_input.currentIndex()),
                  "Daytime/evening attendance": str(self.attendance_input.currentIndex()),
                  "Previous qualification": str(self.previous_qualification_input.currentIndex()),
                  "Nacionality": "Portugal",
                  "Mother's qualification": str(self.mothers_qualification_input.currentIndex()),
                  "Father's qualification": str(self.fathers_qualification_input.currentIndex()),
                  "Mother's occupation": str(self.mothers_occuption_input.currentIndex()),
                  "Father's occupation": str(self.fathers_occuption_input.currentIndex()),
                  "Displaced": "1" if self.displaced_input.currentText() == "Yes" else "0",
                  "Educational special needs": "0",
                  "Debtor": str(self.debtor_input.currentIndex()),
                  "Tuition fees up to date": str(self.tuition_fees_up_to_date_input.currentIndex()),
                  "Gender": str(self.gender_input.currentIndex()),
                  "Scholarship holder": str(self.scholarship_input.currentIndex()),
                  "Age at enrollment": str(self.age_at_enrollment_input.text()),
                  "International": "0",
                  "Curricular units 1st sem (credited)": str(self.Curricular_units_1st_sem_credited_input.text()),
                  "Curricular units 1st sem (enrolled)": str(self.Curricular_units_1st_sem_enrolled_input.text()),
                  "Curricular units 1st sem (evaluations)": str(self.Curricular_units_1st_sem_evaluations_input.text()),
                  "Curricular units 1st sem (approved)": str(self.Curricular_units_1st_sem_approved_input.text()),
                  "Curricular units 1st sem (grade)": str(self.Curricular_units_1st_sem_grade_input.text()),
                  "Curricular units 1st sem (without evaluations)": str(self.Curricular_units_1st_sem_without_evealuations_input.text()),
                  "Curricular units 2nd sem (credited)": str(self.Curricular_units_2nd_sem_credited_input.text()),
                  "Curricular units 2nd sem (enrolled)": str(self.Curricular_units_2nd_sem_enrolled_input.text()),
                  "Curricular units 2nd sem (evaluations)": str(self.Curricular_units_2nd_sem_evaluations_input.text()),
                  "Curricular units 2nd sem (approved)": str(self.Curricular_units_2nd_sem_approved_input.text()),
                  "Curricular units 2nd sem (grade)": str(self.Curricular_units_2nd_sem_grade_input.text()),
                  "Curricular units 2nd sem (without evaluations)": str(self.Curricular_units_2nd_sem_without_evealuations_input.text()),
                  "Unemployment rate": str(self.unemployment_rate_input.text()),
                  "Inflation rate": str(self.inflation_rate_input.text()),
                  "GDP": str(self.gpa_input.text())
            }


            if target:
                student_data["Target"] = str(target)

            print("📤 Sending manual student data:")
            for k, v in student_data.items():
                print(f"{k}: {v} ({type(v)})")

            topic = "manual_dataset_with_target" if target else "manual_dataset_without_target"
            producer.produce(topic, value=json.dumps(student_data).encode('utf-8'))
            producer.flush()

            check_for_new_labeled_data()
            QMessageBox.information(self, "Success", "✅ Student data uploaded successfully!")

        
        except Exception as e:
            QMessageBox.critical(self, "Input Error", f"Error sending data: {e}")

    def reset_manual_form(self):
        self.student_id_input.clear()
        self.name_input.clear()
        self.last_name_input.clear()
        self.martial_input.setCurrentIndex(0)
        self.application_mode_input.setCurrentIndex(0)
        self.application_order_input.clear()
        self.course_input.setCurrentIndex(0)
        self.attendance_input.setCurrentIndex(0)
        self.previous_qualification_input.setCurrentIndex(0)
        self.mothers_qualification_input.setCurrentIndex(0)
        self.fathers_qualification_input.setCurrentIndex(0)
        self.mothers_occuption_input.setCurrentIndex(0)
        self.fathers_occuption_input.setCurrentIndex(0)
        self.displaced_input.setCurrentIndex(0)
        self.debtor_input.setCurrentIndex(0)
        self.tuition_fees_up_to_date_input.setCurrentIndex(0)
        self.gender_input.setCurrentIndex(0)
        self.scholarship_input.setCurrentIndex(0)
        self.age_at_enrollment_input.clear()
        self.Curricular_units_1st_sem_credited_input.clear()
        self.Curricular_units_1st_sem_enrolled_input.clear()
        self.Curricular_units_1st_sem_evaluations_input.clear()
        self.Curricular_units_1st_sem_approved_input.clear()
        self.Curricular_units_1st_sem_grade_input.clear()
        self.Curricular_units_1st_sem_without_evealuations_input.clear()
        self.Curricular_units_2nd_sem_credited_input.clear()
        self.Curricular_units_2nd_sem_enrolled_input.clear()
        self.Curricular_units_2nd_sem_evaluations_input.clear()
        self.Curricular_units_2nd_sem_approved_input.clear()
        self.Curricular_units_2nd_sem_grade_input.clear()
        self.Curricular_units_2nd_sem_without_evealuations_input.clear()
        self.unemployment_rate_input.clear()
        self.inflation_rate_input.clear()
        self.gpa_input.clear()
        self.target_yes_radio.setChecked(False)
        self.target_no_radio.setChecked(False)

    def upload_csv(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV File", "", "CSV Files (*.csv)")

        if not file_path:
            self.csv_status_label.setStyleSheet("""
                background-color: #ffcccc;
                color: #c0392b;
                padding: 10px;
                border-radius: 6px;
                font-weight: bold;
            """)
            self.csv_status_label.setText("⚠️ Upload canceled.")
            return

        try:
            df = pd.read_csv(file_path)

            with_target = 0
            without_target = 0

            for _, row in df.iterrows():
                data = row.to_dict()

                if 'Target' in data and pd.notna(data['Target']) and str(data['Target']).strip() != '':
                    topic = 'manual_dataset_with_target'
                    with_target += 1
                else:
                    topic = 'manual_dataset_without_target'
                    without_target += 1

                producer.produce(topic, value=json.dumps(data).encode('utf-8'))

            producer.flush()
            check_for_new_labeled_data()
            producer.flush()

            self.csv_status_label.setStyleSheet("""
                background-color: #eafaf1;
                color: #27ae60;
                padding: 10px;
                border-radius: 6px;
                font-weight: bold;
            """)
            self.csv_status_label.setText(
                f"✅ Upload complete!\n🎯 Rows with target: {with_target}\n⭕ Rows without target: {without_target}"
            )

        except Exception as e:
            self.csv_status_label.setStyleSheet("""
                background-color: #ffe6e6;
                color: #e74c3c;
                padding: 10px;
                border-radius: 6px;
                font-weight: bold;
            """)
            self.csv_status_label.setText(f"❌ Error while uploading: {str(e)}")



    def apply_theme(self):
        with open("/Users/rubinroy/Desktop/BigDataFinalProject/StudentDropoutPrediction/Views/styles.css", "r") as f:base_styles = f.read() 
        self.setStyleSheet(base_styles)

    def handle_csv_back(self):
        self.csv_status_label.setText("")  
        self.data_tab_stack.setCurrentWidget(self.selection_widget)

    def check_student_prediction(self):
        student_id = self.id_input.text().strip()
        if not student_id:
            self.insight_result_label.setText("❌ Please enter a student ID.")
            self.insight_result_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.insight_result_label.setStyleSheet("color: #e74c3c; font-size: 14px; padding: 10px;")
            return

        try:
            predict_new_student_data()

            consumer = Consumer({
                'bootstrap.servers': 'localhost:9092',
                'group.id': 'dashboard-consumer-group',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            })
            consumer.subscribe(['predicted_manual_dataset'])

            found = False
            while True:
                msg = consumer.poll(5.0)  # 2 second timeout
                if msg is None:
                    break  # no more messages
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                data = json.loads(msg.value().decode('utf-8'))
                if str(data.get("student_id")) == student_id:
                    found = True
                    prob = data.get("dropout_probability", "N/A")
                    self.insight_result_label.setText(
                        f"✅ <b>Student Found</b><br>"
                        f"<b>Name:</b> {data.get('first_name')} {data.get('last_name')}<br>"
                        f"<b>Dropout Probability:</b> {prob * 100:.2f}%"
                    )
                    self.insight_result_label.setStyleSheet("color: #2e86de; font-size: 15px; padding: 15px;")
                    break
                
            if not found:
                self.insight_result_label.setText("⚠️ Student not found in predictions.")
                self.insight_result_label.setStyleSheet("color: #f39c12; font-size: 14px; padding: 10px;")

            self.insight_result_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            consumer.close()

        except Exception as e:
            self.insight_result_label.setText(f"❌ Error: {str(e)}")
            self.insight_result_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.insight_result_label.setStyleSheet("color: #e74c3c; font-size: 14px; padding: 10px;")

