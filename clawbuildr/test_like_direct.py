import sys
sys.path.insert(0, r'C:\Users\marvi\clawbuildr')
from linkedin_engine import auto_like_feed_posts
import json

result = auto_like_feed_posts(max_likes=2)
print(json.dumps(result, indent=2))
