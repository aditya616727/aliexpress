import os
import tempfile
import pytest
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from image_downloader import ImageDownloader


# Fake response for mocking requests
class FakeResponse:
    def __init__(self, content=b"fake_image_data", status_code=200, headers=None):
        self.content = content
        self.status_code = status_code
        self.headers = headers or {"Content-Type": "image/jpeg"}
        self.ok = status_code == 200

    def raise_for_status(self):
        if self.status_code >= 400:
            from requests.exceptions import HTTPError
            raise HTTPError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size=8192):
        yield self.content


class FakeResponsePNG(FakeResponse):
    def __init__(self):
        super().__init__(
            content=b"\x89PNG\r\n\x1a\nfake_png_data",
            headers={"Content-Type": "image/png"},
        )


class TestImageDownloader:

    def setup_method(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.downloader = ImageDownloader(output_dir=self.tmp_dir)

    def teardown_method(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    # --- Filename Sanitization ---

    def test_sanitize_basic(self):
        result = self.downloader._sanitize_filename("Hello World Product")
        assert result == "Hello_World_Product"

    def test_sanitize_special_chars(self):
        result = self.downloader._sanitize_filename('Pro<duct>:Name/"Test"')
        assert "<" not in result
        assert ">" not in result
        assert ":" not in result
        assert '"' not in result

    def test_sanitize_max_length(self):
        long_name = "A" * 200
        result = self.downloader._sanitize_filename(long_name, max_length=80)
        assert len(result) <= 80

    def test_sanitize_empty_string(self):
        result = self.downloader._sanitize_filename("")
        assert isinstance(result, str)

    # --- Extension Detection ---

    def test_get_extension_from_url_jpg(self):
        ext = self.downloader._get_extension("https://example.com/img.jpg")
        assert ext == ".jpg"

    def test_get_extension_from_url_png(self):
        ext = self.downloader._get_extension("https://example.com/img.png")
        assert ext == ".png"

    def test_get_extension_from_url_webp(self):
        ext = self.downloader._get_extension("https://example.com/img.webp")
        assert ext == ".webp"

    def test_get_extension_from_content_type(self):
        ext = self.downloader._get_extension(
            "https://example.com/image", content_type="image/png"
        )
        assert ext == ".png"

    def test_get_extension_default_jpg(self):
        ext = self.downloader._get_extension("https://example.com/unknown")
        assert ext == ".jpg"

    # --- Single Image Download ---

    def test_download_image_success(self, mocker):
        mocker.patch.object(
            self.downloader.session, "get", return_value=FakeResponse()
        )
        path = self.downloader.download_image(
            "https://ae01.alicdn.com/kf/test.jpg",
            product_title="Test Product",
            index=0,
        )
        assert path != ""
        assert os.path.exists(path)
        assert os.path.getsize(path) > 0

    def test_download_image_correct_filename(self, mocker):
        mocker.patch.object(
            self.downloader.session, "get", return_value=FakeResponse()
        )
        path = self.downloader.download_image(
            "https://ae01.alicdn.com/kf/test.jpg",
            product_title="My Product",
            index=5,
        )
        filename = os.path.basename(path)
        assert filename.startswith("0005_")
        assert "My_Product" in filename
        assert filename.endswith(".jpg")

    def test_download_image_png_extension(self, mocker):
        mocker.patch.object(
            self.downloader.session, "get", return_value=FakeResponsePNG()
        )
        path = self.downloader.download_image(
            "https://ae01.alicdn.com/kf/test.png",
            product_title="PNG Image",
            index=1,
        )
        assert path.endswith(".png")

    def test_download_image_no_title_uses_hash(self, mocker):
        mocker.patch.object(
            self.downloader.session, "get", return_value=FakeResponse()
        )
        path = self.downloader.download_image(
            "https://ae01.alicdn.com/kf/test.jpg", product_title="", index=3
        )
        filename = os.path.basename(path)
        assert filename.startswith("0003_")
        assert len(filename) > 10

    def test_download_image_invalid_url(self):
        path = self.downloader.download_image("", product_title="Test", index=0)
        assert path == ""

    def test_download_image_non_http_url(self):
        path = self.downloader.download_image("ftp://example.com/img.jpg", index=0)
        assert path == ""

    def test_download_image_http_error(self, mocker):
        mocker.patch.object(
            self.downloader.session,
            "get",
            return_value=FakeResponse(status_code=404),
        )
        path = self.downloader.download_image(
            "https://ae01.alicdn.com/kf/missing.jpg", index=0
        )
        assert path == ""

    def test_download_image_request_exception(self, mocker):
        import requests as req
        mocker.patch.object(
            self.downloader.session,
            "get",
            side_effect=req.RequestException("Connection failed"),
        )
        path = self.downloader.download_image(
            "https://ae01.alicdn.com/kf/test.jpg", index=0
        )
        assert path == ""

    # --- Batch Download ---

    def test_download_all_success(self, mocker):
        mocker.patch.object(
            self.downloader.session, "get", return_value=FakeResponse()
        )
        products = [
            {"title": "Product A", "image_url": "https://example.com/a.jpg"},
            {"title": "Product B", "image_url": "https://example.com/b.jpg"},
            {"title": "Product C", "image_url": "https://example.com/c.jpg"},
        ]
        downloaded = self.downloader.download_all(products, delay=0)
        assert downloaded == 3
        for p in products:
            assert p["image_path"] != ""
            assert os.path.exists(p["image_path"])

    def test_download_all_skips_missing_urls(self, mocker):
        mocker.patch.object(
            self.downloader.session, "get", return_value=FakeResponse()
        )
        products = [
            {"title": "Product A", "image_url": "https://example.com/a.jpg"},
            {"title": "Product B", "image_url": ""},
            {"title": "Product C", "image_url": "https://example.com/c.jpg"},
        ]
        downloaded = self.downloader.download_all(products, delay=0)
        assert downloaded == 2

    def test_download_all_empty_list(self, mocker):
        downloaded = self.downloader.download_all([], delay=0)
        assert downloaded == 0

    def test_download_all_sets_image_path(self, mocker):
        mocker.patch.object(
            self.downloader.session, "get", return_value=FakeResponse()
        )
        products = [
            {"title": "Test", "image_url": "https://example.com/test.jpg"},
        ]
        self.downloader.download_all(products, delay=0)
        assert products[0]["image_path"] != ""

    def test_download_all_failed_sets_empty_path(self, mocker):
        import requests as req
        mocker.patch.object(
            self.downloader.session,
            "get",
            side_effect=req.RequestException("fail"),
        )
        products = [
            {"title": "Test", "image_url": "https://example.com/test.jpg"},
        ]
        self.downloader.download_all(products, delay=0)
        assert products[0]["image_path"] == ""

    # --- Output Directory ---

    def test_creates_output_directory(self):
        new_dir = os.path.join(self.tmp_dir, "nested", "images")
        downloader = ImageDownloader(output_dir=new_dir)
        assert os.path.isdir(new_dir)

    def test_file_written_to_correct_directory(self, mocker):
        mocker.patch.object(
            self.downloader.session, "get", return_value=FakeResponse()
        )
        path = self.downloader.download_image(
            "https://example.com/test.jpg", product_title="DirTest", index=0
        )
        assert path.startswith(self.tmp_dir)
