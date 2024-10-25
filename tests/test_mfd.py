from http_client.util import make_mfd, BOUNDARY_


def test_make_mfd_with_fields_only():
    fields = {"username": "testuser", "password": "secret"}
    files = {}

    body, content_type = make_mfd(fields, files)

    expected_content_type = f"multipart/form-data; boundary={BOUNDARY_}"

    # Check if boundary is in the body
    assert expected_content_type == content_type
    assert b'Content-Disposition: form-data; name="username"' in body
    assert b'testuser' in body
    assert b'Content-Disposition: form-data; name="password"' in body
    assert b'secret' in body


def test_make_mfd_with_single_file():
    file_content = b"file contents"
    fields = {}
    files = {"file1": [{"filename": "test.txt", "body": file_content}]}

    body, content_type = make_mfd(fields, files)

    expected_content_type = f"multipart/form-data; boundary={BOUNDARY_}"

    assert expected_content_type == content_type
    assert b'Content-Disposition: form-data; name="file1"; filename="test.txt"' in body
    assert file_content in body


def test_make_mfd_with_content_type():
    file_content = b"file contents"
    fields = {}
    files = {"file1": [{"filename": "test.txt", "body": file_content, "content_type": "text/plain"}]}

    body, content_type = make_mfd(fields, files)

    assert b'Content-Disposition: form-data; name="file1"; filename="test.txt"' in body
    assert b'Content-Type: text/plain' in body
    assert file_content in body


def test_make_mfd_with_mixed_fields_and_files():
    file_content = b"file contents"

    fields = {"username": "testuser", "password": "secret"}
    files = {"file1": [{"filename": "test.txt", "body": file_content}]}

    body, content_type = make_mfd(fields, files)

    assert b'Content-Disposition: form-data; name="username"' in body
    assert b'testuser' in body
    assert b'Content-Disposition: form-data; name="password"' in body
    assert b'secret' in body
    assert b'Content-Disposition: form-data; name="file1"; filename="test.txt"' in body
    assert file_content in body
