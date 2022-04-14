import pytest

def test_valuekeydict():
    from sibispy.session import ValueKeyDict
    original =ValueKeyDict({
        "alpha": None,
        "beta": "charlie",
        "delta": "echo"
    })

    assert len(original.keys()) == 3, "Should be 3 keys"
    assert original.get("alpha") == None, "Should be None"
    assert original["alpha"] == "alpha", "Shold be alpha"
    assert original.get("beta") == "charlie", "Should be charlie"
    assert original["beta"] == "charlie", "Should be charlie"
    assert original.get("delta") == "echo", "Should be echo"
    assert original["delta"] == "echo", "Should be echo"

    original.update({
        "alpha": "foxtrot",
        "beta": None,
        "golf": None,
        "hotel": "india"
    })

    assert len(original.keys()) == 5, "Should be 5 keys"
    assert original.get("alpha") == "foxtrot", "Should be foxtrot"
    assert original["alpha"] == "foxtrot", "Shold be foxtrot"
    assert original.get("beta") == None, "Should be None"
    assert original["beta"] == "beta", "Should be beta"
    assert original.get("delta") == "echo", "Should be echo"
    assert original["delta"] == "echo", "Should be echo"
    assert original.get("golf") == None, "Should be None"
    assert original["golf"] == "golf", "Should be golf"
    assert original.get("hotel") == "india", "Should be india"
    assert original["hotel"] == "india", "Should be india"