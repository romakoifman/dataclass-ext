from dataclass_ext.dataclass import dataclass, id_field


# @dataclass(keep_history=True)
@dataclass
class A:
    i_k: int = id_field()
    s_k: str = id_field()
    s: str
    b: int = 10


@dataclass(keep_history=True)
class B:
    i_k: int = id_field()
    s_k: str = id_field()
    s: str
    b: int = 10


@dataclass
class Complex:
    idf: int = id_field()
    la: list[A]


def test_simple():
    a = A(i_k=1, s_k="search", s="str")
    assert a.i_k == 1
    assert a.s == "str"
    assert a.id() == "i_k=1_s_k=search"


def test_storage():
    a = A(i_k=1, s_k="same", s="a1")
    a.save()
    b = A(i_k=2, s_k="same", s="b2")
    b.save()

    from_storage = A.load(i_k=1, s_k="same")
    assert from_storage.s == "a1"

    assert len(A.find()) == 2
    from_storage = A.find(i_k=2)
    assert len(from_storage) == 1
    assert from_storage[0].s == "b2"
    from_storage = A.find(s_k="same")
    assert len(from_storage) == 2
    from_storage = A.find(i_k=1, s_k="same")
    assert len(from_storage) == 1
    assert from_storage[0].s == "a1"

    c = Complex(idf=1, la=[a, b])
    c.save()

    from_storage = Complex.find(idf=1)
    assert len(from_storage[0].la) == 2
    assert from_storage[0].la[0].s_k == "same"


def test_history():
    B.delete_all()

    b = B(i_k=1, s_k="same", s="a1")
    b.save()

    b.s = "a2"
    b.save()

    assert 2 == len(b.history())
