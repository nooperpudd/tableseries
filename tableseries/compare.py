# encoding:utf-8


class DateCompare(object):
    """
    """

    def __init__(self, year=None, month=None, day=None):
        """
        :param year:
        :param month:
        :param day:
        """
        self.year = year
        self.month = month
        self.day = day

    def _compare(self, x, y):
        """
        :param x:
        :param y:
        :return:
        """
        if x == y:
            return 0
        elif x > y:
            return 1
        elif x < y:
            return -1

    def __eq__(self, other):
        # equal ==
        other_year = getattr(other, "year")
        other_month = getattr(other, "month")
        other_day = getattr(other, "day")
        if (self.year, self.month, self.day) == (other_year, other_month, other_day):
            return True
        else:
            return False

    def __le__(self, other):
        # less and equal <=
        other_year = getattr(other, "year")
        other_month = getattr(other, "month")
        other_day = getattr(other, "day")
        if (self.year, self.month, self.day) <= (other_year, other_month, other_day):
            return True
        else:
            return False

    def __lt__(self, other):
        # less than
        other_year = getattr(other, "year")
        other_month = getattr(other, "month")
        other_day = getattr(other, "day")
        if (self.year, self.month, self.day) < (other_year, other_month, other_day):
            return True
        else:
            return False

    def __gt__(self, other):
        # greater than
        other_year = getattr(other, "year")
        other_month = getattr(other, "month")
        other_day = getattr(other, "day")
        if (self.year, self.month, self.day) > (other_year, other_month, other_day):
            return True
        else:
            return False

    def __ge__(self, other):
        # greater and equal
        other_year = getattr(other, "year")
        other_month = getattr(other, "month")
        other_day = getattr(other, "day")
        if (self.year, self.month, self.day) >= (other_year, other_month, other_day):
            return True
        else:
            return False
