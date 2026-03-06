import random
import os
import json
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Initialize OpenAI Client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_subtopics(topic: str, difficulty: str, exclude: str = None) -> list[str]:
    """Dynamically fetch granular PySpark subtopics using the OpenAI LLM."""
    
    exclude_instruction = ""
    if exclude:
        exclude_instruction = f"\n    CRITICAL: You MUST NOT return any of the following previously generated subtopics: {exclude}\n"
    
    prompt = f"""
    You are an expert PySpark instructor creating a practice platform. 
    The user has selected the specific PySpark topic: '{topic}' and the difficulty level: '{difficulty}'.
    {exclude_instruction}
    Return a raw JSON array containing exactly 5-8 highly concise PySpark function names or method names related to this topic that would make good practice problems at this difficulty level. 
    Do NOT include descriptions or explanations in the array strings. JUST the exact method names.
    
    For example, if category is 'Window Functions' and difficulty is 'Medium', return:
    ["rank()", "dense_rank()", "lead()", "lag()", "row_number()", "cume_dist()"]
    
    Return ONLY the raw JSON array of strings. Do not include markdown formatting like ```json.
    """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that outputs raw JSON arrays of strings."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )
        
        raw_content = response.choices[0].message.content.strip()
        
        # Clean up potential markdown formatting if the model disobeys
        if raw_content.startswith("```json"):
            raw_content = raw_content[7:]
        if raw_content.endswith("```"):
            raw_content = raw_content[:-3]
            
        subtopics = json.loads(raw_content.strip())
        return subtopics
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        # Fallback subtopics if API fails
        return [f"Basic {topic}", f"Advanced {topic}", f"Applied {topic}"]

def generate_problem(topic: str = "general", difficulty: str = "Medium"):
    """Dynamically generate a complete PySpark practice problem using LLMs."""
    q_id = f"Q-{random.randint(100, 999)}"
    
    prompt = f"""
    You are an expert PySpark instructor creating a coding challenge for a student.
    The student has requested a practice problem based on the following specific topics: '{topic}'.
    The requested difficulty is: '{difficulty}'.
    
    Design a realistic, engaging scenario that strongly requires the student to write PySpark code utilizing those exact topics/functions to solve the problem.
    
    CRITICAL CONSTRAINTS:
    1. The scenario datasets must contain between 3 to 10 rows.
    2. Include edge cases (like missing values or duplicate keys) if it's relevant to the topic.
    3. The expected_output MUST be exactly what the PySpark final dataframe logic would yield.
    4. Provide the boilerplate `initial_code` that loads the datasets into DataFrames and leaves room for the user to write. Give the DataFrames logical names. ALWAYS append `\n# Your code here\n# NOTE: You MUST assign your final output dataframe to a variable named `final_df`\n` to the bottom of `initial_code`.
    5. VERY IMPORTANT: In the `initial_code` Python string, absolutely ALWAYS use the Python `None` keyword for missing values. NEVER use the JavaScript `null` keyword inside the Python code string!
    
    You MUST output a valid JSON object matching EXACTLY this schema:
    {{
      "title": "String (Short, catchy title for the problem)",
      "description": "String (Markdown formatted text describing the scenario, rules, and final required columns. ALWAYS emphasize that they MUST assign their final result to a variable specifically named `final_df` so the grading engine can read it.)",
      "datasets": {{
        "table1_name": [
          {{"col1": "val", "col2": 1}},
          {{"col1": "val", "col2": 2}}
        ]
      }},
      "expected_output": [
        // array of dicts representing the final correct rows
      ],
      "initial_code": "String (e.g. df_1 = spark.sql('SELECT * FROM table1_name')\\n\\n# Your code here\\n# NOTE: You MUST assign your final output dataframe to a variable named `final_df`\\n)"
    }}
    
    Return ONLY the valid JSON object.
    """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={ "type": "json_object" },
            messages=[
                {"role": "system", "content": "You are a helpful assistant designed to output strict JSON representing a coding challenge."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )
        
        raw_content = response.choices[0].message.content.strip()
        problem_data = json.loads(raw_content)
        
        # Inject the generated ID and difficulty back into the response payload
        problem_data["id"] = q_id
        problem_data["difficulty"] = difficulty
        
        return problem_data
        
    except Exception as e:
        print(f"Error calling OpenAI API for problem generation: {e}")
        # Fallback to a basic problem if the LLM crashes
        return {
            "id": q_id,
            "title": "API Error Fallback Problem",
            "difficulty": difficulty,
            "description": f"The AI failed to generate a problem for {topic}. Here is a fallback.\n\nFilter the `employees` table to find high earners (>100K). Use `df.show()`.",
            "datasets": {
                "employees": [
                    {"id": 1, "name": "Alice", "salary": 120000},
                    {"id": 2, "name": "Bob", "salary": 90000}
                ]
            },
            "expected_output": [
                {"id": 1, "name": "Alice", "salary": 120000}
            ],
            "solution": "final_df = employees_df.filter(employees_df.salary > 100000)"
        }

def generate_sql_subtopics(topic: str, difficulty: str, exclude: str = None) -> list[str]:
    """Dynamically fetch granular SQL subtopics using the OpenAI LLM."""
    exclude_instruction = ""
    if exclude:
        exclude_instruction = f"\n    CRITICAL: You MUST NOT return any of the following previously generated subtopics: {exclude}\n"

    prompt = f"""
    You are an expert SQL instructor creating a practice platform.
    The user has selected the specific SQL topic: '{topic}' and the difficulty level: '{difficulty}'.
    {exclude_instruction}
    Return a raw JSON array containing exactly 5-8 highly concise SQL clause names, keywords, or function names related to this topic that would make good practice problems at this difficulty level.
    Do NOT include descriptions or explanations. JUST the exact SQL keyword/function/clause names.

    For example, if category is 'Window Functions' and difficulty is 'Medium', return:
    ["RANK()", "DENSE_RANK()", "LEAD()", "LAG()", "ROW_NUMBER()", "NTILE()"]

    Return ONLY the raw JSON array of strings. Do not include markdown formatting like ```json.
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that outputs raw JSON arrays of strings."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )
        raw_content = response.choices[0].message.content.strip()
        if raw_content.startswith("```json"):
            raw_content = raw_content[7:]
        if raw_content.endswith("```"):
            raw_content = raw_content[:-3]
        subtopics = json.loads(raw_content.strip())
        return subtopics
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return [f"Basic {topic}", f"Advanced {topic}", f"Applied {topic}"]


def generate_sql_problem(topic: str = "general", difficulty: str = "Medium"):
    """Dynamically generate a complete SQL practice problem using LLMs."""
    q_id = f"Q-{random.randint(100, 999)}"

    prompt = f"""
    You are an expert SQL instructor creating a coding challenge for a student.
    The student has requested a practice problem based on the following specific SQL topics: '{topic}'.
    The requested difficulty is: '{difficulty}'.

    Design a realistic, engaging scenario that strongly requires the student to write a SQL SELECT query utilizing those exact topics/functions.

    CRITICAL CONSTRAINTS:
    1. The scenario datasets must contain between 3 to 10 rows.
    2. Include edge cases (like NULL values or duplicate keys) if relevant to the topic.
    3. The expected_output MUST be exactly what the SQL query would return.
    4. Provide the boilerplate `initial_code` as a SQL SELECT query stub. End it with a comment like:
       -- Write your SQL query here
       SELECT ...
       FROM table_name
    5. The `initial_code` must be a valid SQL string (no Python code, no PySpark references).
    6. Table names in the datasets must match the table names referenced in the initial_code.

    You MUST output a valid JSON object matching EXACTLY this schema:
    {{
      "title": "String (Short, catchy title for the problem)",
      "description": "String (Markdown formatted text describing the scenario, rules, and required output columns.)",
      "datasets": {{
        "table1_name": [
          {{"col1": "val", "col2": 1}},
          {{"col1": "val", "col2": 2}}
        ]
      }},
      "expected_output": [
        // array of dicts representing the final correct rows
      ],
      "initial_code": "-- Write your SQL query here\\nSELECT ...\\nFROM table1_name"
    }}

    Return ONLY the valid JSON object.
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a helpful assistant designed to output strict JSON representing a SQL coding challenge."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )
        raw_content = response.choices[0].message.content.strip()
        problem_data = json.loads(raw_content)
        problem_data["id"] = q_id
        problem_data["difficulty"] = difficulty
        return problem_data
    except Exception as e:
        print(f"Error calling OpenAI API for SQL problem generation: {e}")
        return {
            "id": q_id,
            "title": "API Error Fallback Problem",
            "difficulty": difficulty,
            "description": f"The AI failed to generate a SQL problem for {topic}. Here is a fallback.\n\nFilter the `employees` table to find employees earning more than 100000.",
            "datasets": {
                "employees": [
                    {"id": 1, "name": "Alice", "salary": 120000},
                    {"id": 2, "name": "Bob", "salary": 90000}
                ]
            },
            "expected_output": [
                {"id": 1, "name": "Alice", "salary": 120000}
            ],
            "initial_code": "-- Write your SQL query here\nSELECT *\nFROM employees\nWHERE salary > 100000"
        }


def generate_search_response(query: str) -> str:
    """Uses LLM to act as a semantic search engine for PySpark concepts."""
    prompt = f"""
    You are a Senior PySpark Data Engineer. The user is searching the PySpark documentation/knowledge base for the following concept or question:
    "{query}"
    
    Please provide a highly accurate, concise explanation of this concept. 
    You MUST include at least one clear, well-formatted PySpark code block demonstrating the syntax or usage of the concept.
    Format your entire response strictly in Github-Flavored Markdown. 
    Use bolding for emphasis, and use `inline code` for variable/function names.
    Keep the tone professional, educational, and direct. Do not add conversational filler like "Here is your answer".
    """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a PySpark semantic search engine outputting strict Markdown."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3 # Lower temperature for more factual documentation-style responses
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error calling OpenAI API for semantic search: {e}")
        return f"**Error:** Could not generate search results at this time. Please try again later.\n\n`{e}`"
