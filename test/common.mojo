# Shared test helpers.


def from_hex(s: StringSpan) raises -> List[Byte]:
    var bytes = s.as_bytes()
    if len(bytes) % 2 != 0:
        raise Error("odd-length hex string")
    var out = List[Byte](capacity=len(bytes) // 2)

    def nibble(c: Byte) raises -> Byte:
        var ci = Int(c)
        if ci >= ord("0") and ci <= ord("9"):
            return Byte(ci - ord("0"))
        if ci >= ord("a") and ci <= ord("f"):
            return Byte(ci - ord("a") + 10)
        if ci >= ord("A") and ci <= ord("F"):
            return Byte(ci - ord("A") + 10)
        raise Error("bad hex digit")

    var i = 0
    while i < len(bytes):
        out.append(nibble(bytes[i]) << 4 | nibble(bytes[i + 1]))
        i += 2
    return out^


def to_hex(data: Span[Byte, _]) -> String:
    comptime digits = "0123456789abcdef"
    var out = String()
    for b in data:
        out += digits[byte = Int(b >> 4) : Int(b >> 4) + 1]
        out += digits[byte = Int(b & 0xF) : Int(b & 0xF) + 1]
    return out^
