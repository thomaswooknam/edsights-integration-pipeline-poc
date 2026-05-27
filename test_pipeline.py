import pytest
import responses
from pipeline_poc import fetch_data_from_api, verify_schema_contract

@pytest.fixture
def mock_api_url():
    return "https://api.university.edu/v1/roster"


@responses.activate
def test_successful_api_fetch(mock_api_url):
    """Test that our API layer correctly downloads data from a successful 200 OK web response."""
    mock_payload = "STU_ID,FIRST_NAME\n101,Tommy"
    responses.add(responses.GET, mock_api_url, body=mock_payload, status=200)
    
    data = fetch_data_from_api(mock_api_url)
    assert "STU_ID" in data
    assert "Tommy" in data


@responses.activate
def test_api_server_error_throws_exception(mock_api_url):
    """Test that a 500 Server Error from the API safely catches and raises a RuntimeError."""
    responses.add(responses.GET, mock_api_url, status=500)
    
    with pytest.raises(RuntimeError) as exc_info:
        fetch_data_from_api(mock_api_url)
        
    assert "API Connection Failure" in str(exc_info.value)


def test_schema_drift_from_stream():
    """Test that our contract logic still catches schema drift inside a raw network string stream."""
    expected = ["STU_ID", "FIRST_NAME", "ENROLLMENT_STATUS", "REGISTRATION_DATE"]
    drifted_stream = "STU_ID,FIRST_NAME,MIDDLE_NAME,ENROLLMENT_STATUS,REGISTRATION_DATE\n101,Tommy,Lee,1,2026-05-01"
    
    result = verify_schema_contract(drifted_stream, expected)
    
    assert result["drift_detected"] is True
    assert "MIDDLE_NAME" in result["unexpected_columns"]
