# -*- coding: utf-8 -*-

# Form implementation generated from reading layout file 'otter/gui/layout/main_window.layout'
#
# Created by: PyQt5 UI code generator 5.15.9
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.setWindowModality(QtCore.Qt.ApplicationModal)
        MainWindow.resize(762, 479)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.layoutWidget = QtWidgets.QWidget(self.centralwidget)
        self.layoutWidget.setGeometry(QtCore.QRect(9, 11, 191, 129))
        self.layoutWidget.setObjectName("layoutWidget")
        self.formLayout = QtWidgets.QFormLayout(self.layoutWidget)
        self.formLayout.setContentsMargins(0, 0, 0, 0)
        self.formLayout.setObjectName("formLayout")
        self.label1 = QtWidgets.QLabel(self.layoutWidget)
        font = QtGui.QFont()
        font.setPointSize(10)
        font.setBold(True)
        font.setWeight(75)
        font.setKerning(True)
        self.label1.setFont(font)
        self.label1.setObjectName("label1")
        self.formLayout.setWidget(0, QtWidgets.QFormLayout.LabelRole, self.label1)
        self.edit_user = QtWidgets.QLineEdit(self.layoutWidget)
        self.edit_user.setObjectName("edit_user")
        self.formLayout.setWidget(0, QtWidgets.QFormLayout.FieldRole, self.edit_user)
        self.label2 = QtWidgets.QLabel(self.layoutWidget)
        font = QtGui.QFont()
        font.setPointSize(10)
        font.setBold(True)
        font.setWeight(75)
        font.setKerning(True)
        self.label2.setFont(font)
        self.label2.setObjectName("label2")
        self.formLayout.setWidget(1, QtWidgets.QFormLayout.LabelRole, self.label2)
        self.edit_pass = QtWidgets.QLineEdit(self.layoutWidget)
        self.edit_pass.setEchoMode(QtWidgets.QLineEdit.Password)
        self.edit_pass.setObjectName("edit_pass")
        self.formLayout.setWidget(1, QtWidgets.QFormLayout.FieldRole, self.edit_pass)
        self.check_clear_on_login = QtWidgets.QCheckBox(self.layoutWidget)
        font = QtGui.QFont()
        font.setPointSize(10)
        self.check_clear_on_login.setFont(font)
        self.check_clear_on_login.setObjectName("check_clear_on_login")
        self.formLayout.setWidget(2, QtWidgets.QFormLayout.FieldRole, self.check_clear_on_login)
        self.button_login = QtWidgets.QPushButton(self.layoutWidget)
        font = QtGui.QFont()
        font.setPointSize(10)
        font.setBold(True)
        font.setWeight(75)
        self.button_login.setFont(font)
        self.button_login.setObjectName("button_login")
        self.formLayout.setWidget(3, QtWidgets.QFormLayout.SpanningRole, self.button_login)
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 762, 22))
        self.menubar.setObjectName("menubar")
        self.menu_file = QtWidgets.QMenu(self.menubar)
        self.menu_file.setObjectName("menu_file")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)
        self.action_quit = QtWidgets.QAction(MainWindow)
        self.action_quit.setObjectName("action_quit")
        self.menu_file.addAction(self.action_quit)
        self.menubar.addAction(self.menu_file.menuAction())

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))
        self.label1.setText(_translate("MainWindow", "Username"))
        self.label2.setText(_translate("MainWindow", "Password"))
        self.check_clear_on_login.setText(_translate("MainWindow", "Clear On Login"))
        self.button_login.setText(_translate("MainWindow", "Login"))
        self.menu_file.setTitle(_translate("MainWindow", "File"))
        self.action_quit.setText(_translate("MainWindow", "Quit"))


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())