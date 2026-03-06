"""
Room Manager — Hybrid in-memory WS connections + DB-backed room persistence.
Manages challenge rooms for the Twin Challenge feature.
"""
import string
import random
import json
from datetime import datetime
from fastapi import WebSocket


class RoomManager:
    """
    Manages live WebSocket connections for challenge rooms.
    Room metadata (code, players, problem) lives in the DB.
    This class only tracks ephemeral WS connections and live code state.
    """

    def __init__(self):
        # { room_code: { "connections": {username: WebSocket}, "code": {username: str} } }
        self.active_rooms: dict = {}

    def generate_room_code(self) -> str:
        """Generate a unique 6-character alphanumeric room code."""
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

    def register_connection(self, room_code: str, username: str, ws: WebSocket):
        """Register a WebSocket connection for a user in a room."""
        if room_code not in self.active_rooms:
            self.active_rooms[room_code] = {"connections": {}, "code": {}}
        self.active_rooms[room_code]["connections"][username] = ws
        self.active_rooms[room_code]["code"].setdefault(username, "")

    def remove_connection(self, room_code: str, username: str):
        """Remove a user's WebSocket connection from a room."""
        if room_code in self.active_rooms:
            self.active_rooms[room_code]["connections"].pop(username, None)
            # Clean up empty rooms from memory (DB record persists)
            if not self.active_rooms[room_code]["connections"]:
                del self.active_rooms[room_code]

    def update_code(self, room_code: str, username: str, code: str):
        """Update a user's live code in memory."""
        if room_code in self.active_rooms:
            self.active_rooms[room_code]["code"][username] = code

    def get_opponent_code(self, room_code: str, username: str) -> str:
        """Get the opponent's current code."""
        if room_code not in self.active_rooms:
            return ""
        code_map = self.active_rooms[room_code]["code"]
        for user, code in code_map.items():
            if user != username:
                return code
        return ""

    async def broadcast(self, room_code: str, message: dict, exclude: str = None):
        """Send a message to all connected users in a room, optionally excluding one."""
        if room_code not in self.active_rooms:
            return
        connections = self.active_rooms[room_code]["connections"]
        dead_connections = []
        for username, ws in connections.items():
            if username == exclude:
                continue
            try:
                await ws.send_json(message)
            except Exception:
                dead_connections.append(username)
        # Clean up dead connections
        for username in dead_connections:
            connections.pop(username, None)

    async def send_to_user(self, room_code: str, username: str, message: dict):
        """Send a message to a specific user in a room."""
        if room_code not in self.active_rooms:
            return
        ws = self.active_rooms[room_code]["connections"].get(username)
        if ws:
            try:
                await ws.send_json(message)
            except Exception:
                pass

    def get_connected_usernames(self, room_code: str) -> list:
        """Get list of currently connected usernames in a room."""
        if room_code not in self.active_rooms:
            return []
        return list(self.active_rooms[room_code]["connections"].keys())


# Global singleton
room_manager = RoomManager()
