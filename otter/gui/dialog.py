from typing import Callable

from PyQt5.QtWidgets import QWidget, QMessageBox


def yes_or_no(parent: QWidget, question: str = "Are you sure?", yes: Callable = lambda: None, no: Callable = lambda: None) -> Callable:
    def trigger() -> None:
        if QMessageBox.question(parent, "", question) == QMessageBox.Yes:
            yes()
        else:
            no()
    return trigger
