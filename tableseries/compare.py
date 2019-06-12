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

    def __eq__(self, other):
        # equal ==
        other_year = getattr(other, "year")
        other_month = getattr(other, "month")
        other_day = getattr(other, "day")
        return (self.year, self.month, self.day) == (other_year, other_month, other_day)

    def __le__(self, other):
        # less and equal <=
        other_year = getattr(other, "year")
        other_month = getattr(other, "month")
        other_day = getattr(other, "day")
        return (self.year, self.month, self.day) <= (other_year, other_month, other_day)

    def __lt__(self, other):
        # less than
        other_year = getattr(other, "year")
        other_month = getattr(other, "month")
        other_day = getattr(other, "day")
        return (self.year, self.month, self.day) < (other_year, other_month, other_day)

    def __gt__(self, other):
        # greater than >-
        other_year = getattr(other, "year")
        other_month = getattr(other, "month")
        other_day = getattr(other, "day")
        return (self.year, self.month, self.day) > (other_year, other_month, other_day)

    def __ge__(self, other):
        # greater and equal
        other_year = getattr(other, "year")
        other_month = getattr(other, "month")
        other_day = getattr(other, "day")
        return (self.year, self.month, self.day) >= (other_year, other_month, other_day)

    def __repr__(self):
        return "{0.year}-{0.month}-{0.day}".format(self)
