import os
import pytest
from pipeline_poc import verify_schema_contract

@pytest.fixture
def create_mock_csv(tmp_path):
    """Fixture to dynamically generate temporary CSV files for testing."""
    def _create_file(headers_line):
        file_path = tmp_path / "test_roster.csv"
        file_path.write_text(f"{headers_line}\n101,Tommy,1,2026-05-01")
        return str(file_path)
    return _create_file


def test_perfect_schema_match(create_mock_csv):
    """Test that a perfectly matching schema returns no drift."""
    expected = ["STU_ID", "FIRST_NAME", "ENROLLMENT_STATUS", "REGISTRATION_DATE"]
    valid_file = create_mock_csv("STU_ID,FIRST_NAME,ENROLLMENT_STATUS,REGISTRATION_DATE")
    
    result = verify_schema_contract(valid_file, expected)
    
    assert result["drift_detected"] is False
    assert len(result["unexpected_columns"]) == 0


def test_schema_drift_detection(create_mock_csv):
    """Test that an added rogue column is successfully flagged as drift."""
    expected = ["STU_ID", "FIRST_NAME", "ENROLLMENT_STATUS", "REGISTRATION_DATE"]
    drifted_file = create_mock_csv("STU_ID,FIRST_NAME,MIDDLE_NAME,ENROLLMENT_STATUS,REGISTRATION_DATE")
    
    result = verify_schema_contract(drifted_file, expected)
    
    assert result["drift_detected"] is True
    assert "MIDDLE_NAME" in result["unexpected_columns"]


def test_missing_column_throws_error(create_mock_csv):
    """Test that a missing required column safely halts execution by raising a ValueError."""
    expected = ["STU_ID", "FIRST_NAME", "ENROLLMENT_STATUS", "REGISTRATION_DATE"]
    broken_file = create_mock_csv("STU_ID,FIRST_NAME,ENROLLMENT_STATUS")
    
    with pytest.raises(ValueError) as exc_info:
        verify_schema_contract(broken_file, expected)
        
    assert "Missing required columns" in str(exc_info.value)
