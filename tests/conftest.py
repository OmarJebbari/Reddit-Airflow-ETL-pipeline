import pytest
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def reddit_post_payload():
    return {
        "id": "abc123",
        "title": "Example title",
        "selftext": "Body",
        "score": 10,
        "num_comments": 4,
        "author": "user1",
        "created_utc": 1710000000,
        "url": "https://reddit.com/r/test",
        "upvote_ratio": 0.95,
        "over_18": False,
        "edited": False,
        "spoiler": False,
        "stickied": False,
    }
