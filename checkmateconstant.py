#! /usr/bin/python

import inspect

def constant(f):
    def fset(self, value):
        try:
            # try to get line of code that caused this error.
            # it will look something like cmc.SomeFunc = 'some_value'
            fun_name = inspect.stack()[1][4][0]
            # get everything after the dot.
            fun_name = fun_name.split('.')[1]
            # get everything before the equals sign.
            fun_name = fun_name.split('=')[0]
            # remove any white space from front and back.
            fun_name = fun_name.strip(' ')
        except Exception, e:
            # set a default in case we have an error above.
            fun_name = 'Cannot access property bc property'

        raise CheckMateConfigError(fun_name)

    def fget(self):
        return f()

    return property(fget, fset)


class CheckMateConfigError(Exception):
    def __init__(self, fun_name):
        self.value = "Error - {0} is a constant.".format(fun_name)

    def __str__(self):
        return repr(self.value)

