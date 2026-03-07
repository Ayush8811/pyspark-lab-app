import React, { useState, useEffect, useContext } from 'react';
import { ArrowLeft, Search, CheckCircle, Circle, Filter, SortAsc, Eye, X, Code, BookOpen, ChevronDown } from 'lucide-react';
import axios from 'axios';
import { AuthContext } from './AuthContext';
import { API_BASE_URL } from './config';
import './PracticeList.css';

const WINDOW_FUNCTION_TYPES = [
    'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE',
    'LAG', 'LEAD', 'SUM', 'AVG', 'COUNT',
    'FIRST_VALUE', 'LAST_VALUE', 'PERCENT_RANK', 'CUME_DIST'
];

const DIFFICULTY_COLORS = {
    Easy: '#10b981',
    Medium: '#f59e0b',
    Hard: '#ef4444',
};

const PracticeList = ({ onBack, onOpenProblem }) => {
    const { token } = useContext(AuthContext);
    const [problems, setProblems] = useState([]);
    const [loading, setLoading] = useState(true);
    const [searchQuery, setSearchQuery] = useState('');
    const [selectedTypes, setSelectedTypes] = useState([]);
    const [selectedDifficulty, setSelectedDifficulty] = useState(null);
    const [sortBy, setSortBy] = useState('order');
    const [language, setLanguage] = useState('pyspark');
    const [solutionModal, setSolutionModal] = useState(null);
    const [solutionData, setSolutionData] = useState(null);
    const [loadingSolution, setLoadingSolution] = useState(false);
    const [solutionTab, setSolutionTab] = useState('pyspark');
    const [showFilters, setShowFilters] = useState(true);

    useEffect(() => {
        fetchProblems();
    }, []);

    const fetchProblems = async () => {
        setLoading(true);
        try {
            const headers = token ? { Authorization: `Bearer ${token}` } : {};
            const res = await axios.get(`${API_BASE_URL}/api/practice-list/problems?track=window_functions`, { headers });
            setProblems(res.data);
        } catch (err) {
            console.error('Failed to fetch practice list problems:', err);
        } finally {
            setLoading(false);
        }
    };

    const handleViewSolution = async (problem) => {
        setSolutionModal(problem);
        setLoadingSolution(true);
        setSolutionTab('pyspark');
        try {
            const res = await axios.get(`${API_BASE_URL}/api/practice-list/problems/${problem.id}/solution`);
            setSolutionData(res.data);
        } catch (err) {
            console.error('Failed to fetch solution:', err);
        } finally {
            setLoadingSolution(false);
        }
    };

    const handleOpenProblem = async (problem) => {
        try {
            const headers = token ? { Authorization: `Bearer ${token}` } : {};
            const res = await axios.get(`${API_BASE_URL}/api/practice-list/problems/${problem.id}`, { headers });
            onOpenProblem(res.data, language);
        } catch (err) {
            console.error('Failed to fetch problem detail:', err);
        }
    };

    const toggleType = (type) => {
        setSelectedTypes(prev =>
            prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]
        );
    };

    // Filter and sort problems
    let filtered = [...problems];
    if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase();
        filtered = filtered.filter(p =>
            p.title.toLowerCase().includes(q) ||
            p.window_function_type.toLowerCase().includes(q) ||
            p.use_case_category.toLowerCase().includes(q)
        );
    }
    if (selectedTypes.length > 0) {
        filtered = filtered.filter(p => selectedTypes.includes(p.window_function_type));
    }
    if (selectedDifficulty) {
        filtered = filtered.filter(p => p.difficulty === selectedDifficulty);
    }

    if (sortBy === 'difficulty') {
        const order = { Easy: 0, Medium: 1, Hard: 2 };
        filtered.sort((a, b) => order[a.difficulty] - order[b.difficulty]);
    } else if (sortBy === 'type') {
        filtered.sort((a, b) => a.window_function_type.localeCompare(b.window_function_type));
    }

    const solvedCount = problems.filter(p => p.solved_pyspark || p.solved_sql).length;
    const totalCount = problems.length;

    return (
        <div className="practice-list-container">
            {/* Background */}
            <div className="space-background">
                <div className="glow-orb primary"></div>
                <div className="glow-orb secondary"></div>
                <div className="stars"></div>
            </div>

            {/* Header */}
            <div className="practice-list-header">
                <div className="practice-header-top">
                    <button className="practice-back-btn" onClick={onBack}>
                        <ArrowLeft size={20} />
                        <span>Back</span>
                    </button>
                    <div className="practice-language-toggle">
                        <button
                            className={`lang-btn ${language === 'pyspark' ? 'active' : ''}`}
                            onClick={() => setLanguage('pyspark')}
                        >
                            PySpark
                        </button>
                        <button
                            className={`lang-btn ${language === 'sql' ? 'active' : ''}`}
                            onClick={() => setLanguage('sql')}
                        >
                            SQL
                        </button>
                    </div>
                </div>

                <div className="practice-header-info">
                    <h1 className="practice-title">
                        <BookOpen size={28} style={{ color: '#a855f7' }} />
                        Window Functions
                    </h1>
                    <p className="practice-subtitle">
                        Master ROW_NUMBER, RANK, LAG, LEAD, running totals, and more with {totalCount} curated real-world problems.
                    </p>
                    <div className="practice-progress-section">
                        <div className="practice-progress-bar">
                            <div
                                className="practice-progress-fill"
                                style={{ width: totalCount > 0 ? `${(solvedCount / totalCount) * 100}%` : '0%' }}
                            />
                        </div>
                        <span className="practice-progress-text">
                            {solvedCount} / {totalCount} solved
                        </span>
                    </div>
                </div>
            </div>

            {/* Body: Filters + Grid */}
            <div className="practice-list-body">
                {/* Sidebar Filters */}
                <div className={`practice-filters ${showFilters ? 'open' : 'collapsed'}`}>
                    <button className="filter-toggle-btn" onClick={() => setShowFilters(!showFilters)}>
                        <Filter size={16} />
                        <span>Filters</span>
                        <ChevronDown size={14} className={`chevron ${showFilters ? 'open' : ''}`} />
                    </button>

                    {showFilters && (
                        <>
                            {/* Search */}
                            <div className="filter-section">
                                <div className="filter-search-wrapper">
                                    <Search size={14} className="filter-search-icon" />
                                    <input
                                        type="text"
                                        placeholder="Search problems..."
                                        value={searchQuery}
                                        onChange={(e) => setSearchQuery(e.target.value)}
                                        className="filter-search-input"
                                    />
                                </div>
                            </div>

                            {/* Difficulty */}
                            <div className="filter-section">
                                <h4 className="filter-label">Difficulty</h4>
                                <div className="filter-pills">
                                    {['Easy', 'Medium', 'Hard'].map(d => (
                                        <button
                                            key={d}
                                            className={`filter-pill ${selectedDifficulty === d ? 'active' : ''}`}
                                            style={selectedDifficulty === d ? { backgroundColor: DIFFICULTY_COLORS[d], borderColor: DIFFICULTY_COLORS[d] } : {}}
                                            onClick={() => setSelectedDifficulty(selectedDifficulty === d ? null : d)}
                                        >
                                            {d}
                                        </button>
                                    ))}
                                </div>
                            </div>

                            {/* Window Function Type */}
                            <div className="filter-section">
                                <h4 className="filter-label">Function Type</h4>
                                <div className="filter-type-list">
                                    {WINDOW_FUNCTION_TYPES.map(type => (
                                        <label key={type} className="filter-checkbox">
                                            <input
                                                type="checkbox"
                                                checked={selectedTypes.includes(type)}
                                                onChange={() => toggleType(type)}
                                            />
                                            <span className="filter-checkbox-label">{type}</span>
                                        </label>
                                    ))}
                                </div>
                            </div>

                            {/* Sort */}
                            <div className="filter-section">
                                <h4 className="filter-label">Sort By</h4>
                                <div className="filter-pills">
                                    {[{ key: 'order', label: 'Default' }, { key: 'difficulty', label: 'Difficulty' }, { key: 'type', label: 'Type' }].map(s => (
                                        <button
                                            key={s.key}
                                            className={`filter-pill ${sortBy === s.key ? 'active' : ''}`}
                                            onClick={() => setSortBy(s.key)}
                                        >
                                            {s.label}
                                        </button>
                                    ))}
                                </div>
                            </div>

                            {/* Clear Filters */}
                            {(searchQuery || selectedTypes.length > 0 || selectedDifficulty) && (
                                <button
                                    className="clear-filters-btn"
                                    onClick={() => {
                                        setSearchQuery('');
                                        setSelectedTypes([]);
                                        setSelectedDifficulty(null);
                                        setSortBy('order');
                                    }}
                                >
                                    Clear All Filters
                                </button>
                            )}
                        </>
                    )}
                </div>

                {/* Problem Grid */}
                <div className="practice-grid-area">
                    {loading ? (
                        <div className="practice-loading">Loading problems...</div>
                    ) : filtered.length === 0 ? (
                        <div className="practice-empty">No problems match your filters.</div>
                    ) : (
                        <div className="practice-grid">
                            {filtered.map(problem => {
                                const isSolved = language === 'sql' ? problem.solved_sql : problem.solved_pyspark;
                                const otherSolved = language === 'sql' ? problem.solved_pyspark : problem.solved_sql;

                                return (
                                    <div
                                        key={problem.id}
                                        className={`practice-tile ${isSolved ? 'solved' : ''}`}
                                        onClick={() => handleOpenProblem(problem)}
                                    >
                                        <div className="tile-header">
                                            <span
                                                className="tile-difficulty"
                                                style={{ color: DIFFICULTY_COLORS[problem.difficulty] }}
                                            >
                                                {problem.difficulty}
                                            </span>
                                            <div className="tile-solved-indicator">
                                                {isSolved ? (
                                                    <CheckCircle size={18} className="solved-icon" />
                                                ) : otherSolved ? (
                                                    <Circle size={18} className="half-solved-icon" />
                                                ) : null}
                                            </div>
                                        </div>

                                        <h3 className="tile-title">{problem.title}</h3>

                                        <div className="tile-tags">
                                            <span className="tile-wf-badge">{problem.window_function_type}</span>
                                            <span className="tile-usecase-tag">{problem.use_case_category}</span>
                                        </div>

                                        <button
                                            className="tile-solution-btn"
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                handleViewSolution(problem);
                                            }}
                                        >
                                            <Eye size={14} />
                                            View Solution
                                        </button>
                                    </div>
                                );
                            })}
                        </div>
                    )}
                </div>
            </div>

            {/* Solution Modal */}
            {solutionModal && (
                <div className="solution-modal-overlay" onClick={() => { setSolutionModal(null); setSolutionData(null); }}>
                    <div className="solution-modal" onClick={(e) => e.stopPropagation()}>
                        <div className="solution-modal-header">
                            <h2>{solutionModal.title}</h2>
                            <button className="solution-close-btn" onClick={() => { setSolutionModal(null); setSolutionData(null); }}>
                                <X size={20} />
                            </button>
                        </div>

                        {loadingSolution ? (
                            <div className="solution-loading">Loading solution...</div>
                        ) : solutionData ? (
                            <div className="solution-modal-body">
                                {/* Language Tabs */}
                                <div className="solution-tabs">
                                    <button
                                        className={`solution-tab ${solutionTab === 'pyspark' ? 'active' : ''}`}
                                        onClick={() => setSolutionTab('pyspark')}
                                    >
                                        <Code size={14} />
                                        PySpark
                                    </button>
                                    <button
                                        className={`solution-tab ${solutionTab === 'sql' ? 'active' : ''}`}
                                        onClick={() => setSolutionTab('sql')}
                                    >
                                        <Code size={14} />
                                        SQL
                                    </button>
                                </div>

                                {/* Code Block */}
                                <div className="solution-code-block">
                                    <pre className="solution-code">
                                        {solutionTab === 'pyspark'
                                            ? solutionData.solution_code_pyspark
                                            : solutionData.solution_code_sql
                                        }
                                    </pre>
                                </div>

                                {/* Explanation */}
                                <div className="solution-explanation">
                                    <h3>Explanation</h3>
                                    <div className="solution-explanation-text">
                                        {solutionData.explanation}
                                    </div>
                                </div>
                            </div>
                        ) : (
                            <div className="solution-loading">Failed to load solution.</div>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
};

export default PracticeList;
