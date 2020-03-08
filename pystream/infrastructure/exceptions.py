class EmptyNullableException(Exception):
    pass


class SuppliedNoneException(Exception):
    message = "Element found is None!"
