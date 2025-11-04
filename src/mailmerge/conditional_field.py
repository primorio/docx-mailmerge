import operator
import re
import warnings

from .field import BaseMergeField, NextRecord, SkipRecord

# from operator import le, lt, ge, gt, eq, ne


def my_eq(op1, op2):
    try:
        op1_float = float(op1)
        op2_float = float(op2)
        return operator.eq(op1_float, op2_float)
    except ValueError:
        pass

    # convert regexp
    op2 = re.escape(op2).replace("\\*", ".*").replace("\\?", ".")
    # print(f"my_eq '{op1}' '{op2}'", "result", re.match(op2, op1, re.IGNORECASE))
    return re.fullmatch(op2, op1, re.IGNORECASE)


def my_ne(op1, op2):
    # print(f"check my_ne '{op1}' # '{op2}' ")
    return not my_eq(op1, op2)


OP_MAP = {
    "<=": operator.le,
    "<": operator.lt,
    ">": operator.gt,
    ">=": operator.ge,
    "=": my_eq,
    "<>": my_ne,
    "!=": my_ne,
}


class ConditionalField(BaseMergeField):
    def fill_data(self, merge_data, row):
        if self._nested_elements:
            self._fill_nested_elements(merge_data, row)
        # print("|".join(self.current_instr_tokens))
        self.filled_elements = []
        if self.check_condition():
            return self.return_true()
        return self.return_false()

    def check_condition(self):
        operator1, operator, operator2 = self.current_instr_tokens[1:4]
        result = self.check_operator(operator1, operator, operator2)
        # print(f"result condition '{operator1}' '{operator}' '{operator2}' = {result}")
        return result

    def check_operator(self, operator1, operator, operator2):
        # TODO keep double quotes in the operators
        operator_func = OP_MAP.get(operator)
        if operator_func is None:
            warnings.warn(f"Invalid operator {operator}")
            return False
        return operator_func(operator1, operator2)

    def return_true(self):
        return

    def return_false(self):
        return


class NextIfField(ConditionalField):
    def return_true(self):
        raise NextRecord()


class SkipIfField(ConditionalField):
    def return_true(self):
        raise SkipRecord()


class IfField(ConditionalField):
    def return_true(self):
        value_true = self.current_instr_tokens[4] if self.current_instr_tokens[4:] else ""
        # print("value_true", value_true)
        self.fill_value(self._instr_elements[0], value_true)

    def return_false(self):
        value_false = self.current_instr_tokens[5] if self.current_instr_tokens[5:] else ""
        # print("value_false", value_false)
        self.fill_value(self._instr_elements[0], value_false)
