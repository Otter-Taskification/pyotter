from PyQt5 import QtWidgets as qtw

from .main_window import MainWindow


class App(qtw.QApplication):

    def __init__(self, argv: list[str]):
        super().__init__(argv)
        self.window = MainWindow(quit_action=self.quit)

    def run(self) -> int:
        self.window.show()
        return self.exec_()
