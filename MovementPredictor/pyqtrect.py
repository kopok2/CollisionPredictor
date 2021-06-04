import pyqtgraph as pg
from PyQt5 import QtGui, QtCore


class RectItem(pg.GraphicsObject):
    def __init__(self, rect, pencl, fill=None, parent=None):
        super().__init__(parent)
        self._rect = rect
        self.picture = QtGui.QPicture()
        self.pencl = pencl
        self.fill = fill
        self._generate_picture()

    @property
    def rect(self):
        return self._rect

    def _generate_picture(self):
        painter = QtGui.QPainter(self.picture)
        painter.setPen(pg.mkPen(self.pencl))
        painter.setBrush(pg.mkBrush(self.fill))
        painter.drawRect(self.rect)
        painter.end()

    def paint(self, painter, option, widget=None):
        painter.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        return QtCore.QRectF(self.picture.boundingRect())