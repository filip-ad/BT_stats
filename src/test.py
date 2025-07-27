from models.player import Player
from db import get_conn
conn, cursor = get_conn()
player = Player.get_by_id_ext(cursor, 81837)
print(player)
conn.close()