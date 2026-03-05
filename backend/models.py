from sqlalchemy import Column, Integer, String, Text, ForeignKey, JSON
from sqlalchemy.orm import relationship
from database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    
    # Phase 11 Settings Profile Info
    name = Column(String, nullable=True)
    age = Column(Integer, nullable=True)
    bio = Column(Text, nullable=True)

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
