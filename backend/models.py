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
    practice_progress = relationship("PracticeListProgress", back_populates="user")

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

class PracticeListProblem(Base):
    __tablename__ = "practice_list_problems"

    id = Column(Integer, primary_key=True, index=True)
    track = Column(String, index=True)  # e.g., "window_functions"

    # Problem metadata
    title = Column(String, nullable=False)
    description = Column(Text, nullable=False)  # Markdown
    difficulty = Column(String, nullable=False)  # "Easy" | "Medium" | "Hard"
    window_function_type = Column(String, nullable=False)  # "ROW_NUMBER", "RANK", etc.
    use_case_category = Column(String, nullable=False)  # "E-Commerce", "HR", etc.
    order_index = Column(Integer, default=0)

    # Problem data
    datasets = Column(JSON, nullable=False)
    expected_output = Column(JSON, nullable=False)

    # PySpark variant
    initial_code_pyspark = Column(Text, nullable=False)
    solution_code_pyspark = Column(Text, nullable=False)

    # SQL variant
    initial_code_sql = Column(Text, nullable=False)
    solution_code_sql = Column(Text, nullable=False)

    # Explanation
    explanation = Column(Text, nullable=False)  # Markdown

class PracticeListProgress(Base):
    __tablename__ = "practice_list_progress"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    problem_id = Column(Integer, ForeignKey("practice_list_problems.id"), nullable=False)
    language = Column(String, nullable=False)  # "pyspark" | "sql"
    solved_at = Column(String, nullable=False)  # ISO timestamp

    user = relationship("User", back_populates="practice_progress")
    problem = relationship("PracticeListProblem")

