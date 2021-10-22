from src.helpers.navigation import get_directories, get_files


class TestGetDirectories:
    def test_get_directories(self, tmpdir):
        tmpdir.mkdir("potato")
        tmpdir.mkdir("banana")
        p = tmpdir.join("hello.txt")
        p.write("hi")
        assert [_.name for _ in get_directories(tmpdir)] == ["potato", "banana"]


class TestGetFiles:
    def test_get_files(self, tmpdir):
        tmpdir.mkdir("potato")
        tmpdir.mkdir("banana")
        p = tmpdir.join("hello.txt")
        p.write("hi")
        assert [_.name for _ in get_files(tmpdir)] == ["hello.txt"]
