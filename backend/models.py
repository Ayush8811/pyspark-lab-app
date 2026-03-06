from sqlalchemy import Column, Integer, String, Text, ForeignKey, JSON
from sqlalchemy.orm import relationship
from database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True, nullable=True)
    hashed_password = Column(String)
    
    # Phase 11 Settings Profile Info
    name = Column(String, nullable=True)
    age = Column(Integer, nullable=True)
    bio = Column(Text, nullable=True)
    
    # Forgot Password variables
    reset_code = Column(String, nullable=True)
    reset_code_expires = Column(String, nullable=True) # store as ISO string for simplicity

    saved_problems = relationship("SavedProblem", back_populates="owner")
    activity_logs = relationship("ActivityLog", back_populates="owner")

class SavedProblem(Base):
    __tablename__ = "saved_problems"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    
    # Challenge Meta
    title = Column(String)
    description = Column(Text)
    difficulty = Column(String)
    tags = Column(String) # String representation of tags, "Topic - Subtopic"
    
    # Challenge Data
    datasets = Column(JSON)
    expected_output = Column(JSON)
    initial_code = Column(Text)
    language = Column(String, default="pyspark")  # "pyspark" | "sql"
    
    # Link to parent
    owner = relationship("User", back_populates="saved_problems")

class ActivityLog(Base):
    __tablename__ = "activity_logs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    date = Column(String, index=True) # YYYY-MM-DD format
    difficulty = Column(String)
    problem_title = Column(String, nullable=True)
    
    # Link to parent
    owner = relationship("User", back_populates="activity_logs")

class ChallengeRoom(Base):
    __tablename__ = "challenge_rooms"

    id = Column(Integer, primary_key=True, index=True)
    room_code = Column(String, unique=True, index=True)
    creator_id = Column(Integer, ForeignKey("users.id"))
    joiner_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    status = Column(String, default="waiting")  # waiting | active | finished
    problem_data = Column(JSON, nullable=True)  # full problem payload
    language = Column(String, default="pyspark")  # pyspark | sql
    created_at = Column(String)  # ISO timestamp

    creator = relationship("User", foreign_keys=[creator_id])
    joiner = relationship("User", foreign_keys=[joiner_id])

