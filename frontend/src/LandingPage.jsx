import React, { useState, useRef, useEffect } from 'react';
import axios from 'axios';
import { Sparkles, TerminalSquare, CheckCircle, Flame, ArrowRight, User, Code, Search, LogOut, Settings, LayoutDashboard } from 'lucide-react';
import SearchResultModal from './SearchResultModal';
import SettingsModal from './SettingsModal';
import { API_BASE_URL } from './config';
import './LandingPage.css';

const LandingPage = ({ onStartPracticing, onShowAuthModal, onShowProfileModal, user, onLogout }) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [isSearching, setIsSearching] = useState(false);
    const [searchResult, setSearchResult] = useState(null);
    const [searchError, setSearchError] = useState(null);
    const [showSearchModal, setShowSearchModal] = useState(false);

    // Dropdown & Settings State
    const [showDropdown, setShowDropdown] = useState(false);
    const [showSettingsModal, setShowSettingsModal] = useState(false);
    const dropdownRef = useRef(null);

    // Close dropdown when clicking outside
    useEffect(() => {
        function handleClickOutside(event) {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
                setShowDropdown(false);
            }
        }
        document.addEventListener("mousedown", handleClickOutside);
        return () => document.removeEventListener("mousedown", handleClickOutside);
    }, [dropdownRef]);

    const handleSearch = async () => {
        if (!searchQuery.trim()) return;

        setIsSearching(true);
        setSearchError(null);
        setShowSearchModal(true);

        try {
            const res = await axios.post(`${API_BASE_URL}/api/search`, {
                query: searchQuery
            });
            setSearchResult(res.data.markdown);
        } catch (err) {
            setSearchError(err.response?.data?.detail || 'Failed to connect to the AI Search Engine.');
        } finally {
            setIsSearching(false);
        }
    };

    const handleKeyDown = (e) => {
        if (e.key === 'Enter') {
            handleSearch();
        }
    };

    return (
        <div className="landing-page-container">
            {/* Dynamic Background */}
            <div className="space-background">
                <div className="glow-orb primary"></div>
                <div className="glow-orb secondary"></div>
                <div className="stars"></div>
                {/* Placeholder for floating assets like an astronaut or rocket */}
                <div className="floating-asset planet"></div>
            </div>

            {/* Glassmorphic Navigation */}
            <nav className="glass-nav">
                <div className="nav-logo">
                    <Sparkles className="logo-icon" size={24} />
                    <span className="logo-text">DataLab</span>
                </div>

                <div className="nav-links">
                    <a href="#" className="active">Home</a>
                    <a href="#features">Features</a>
                    <a href="#" onClick={(e) => { e.preventDefault(); onStartPracticing('pyspark'); }}>Sandbox</a>
                </div>

                <div className="nav-auth">
                    {user ? (
                        <div className="user-menu-container" ref={dropdownRef}>
                            <button
                                className="icon-btn"
                                style={{ padding: '0.4rem', color: 'var(--accent)', background: 'rgba(168, 85, 247, 0.1)', marginRight: '1rem' }}
                                onClick={() => setShowDropdown(!showDropdown)}
                            >
                                <User size={20} />
                            </button>

                            {/* User Dropdown Menu */}
                            {showDropdown && (
                                <div className="user-dropdown">
                                    <div className="dropdown-header">
                                        <div className="user-name">{user.name || user.username}</div>
                                        <div className="user-role">@{user.username}</div>
                                    </div>
                                    <button className="dropdown-item" onClick={() => { setShowDropdown(false); onShowProfileModal(); }}>
                                        <LayoutDashboard size={14} /> My Dashboard
                                    </button>
                                    <button className="dropdown-item" onClick={() => { setShowDropdown(false); setShowSettingsModal(true); }}>
                                        <Settings size={14} /> Settings
                                    </button>
                                    <button className="dropdown-item logout" onClick={() => { setShowDropdown(false); onLogout(); }}>
                                        <LogOut size={14} /> Sign Out
                                    </button>
                                </div>
                            )}

                            <button className="glass-btn primary-gradient" onClick={() => onStartPracticing('pyspark')}>
                                Enter IDE <ArrowRight size={16} style={{ marginLeft: '6px' }} />
                            </button>
                        </div>
                    ) : (
                        <>
                            <button className="glass-btn text-only" onClick={onShowAuthModal}>Sign In</button>
                            <button className="glass-btn primary-gradient" onClick={onShowAuthModal}>Register</button>
                        </>
                    )}
                </div>
            </nav>

            {/* Hero Section */}
            <div className="hero-section">
                <div className="hero-badge">🚀 The Ultimate Data Engineering Learning Destination</div>
                <h1 className="hero-title">
                    Master <span className="text-gradient">PySpark & SQL</span> — in your browser
                </h1>
                <p className="hero-subtitle">
                    AI-generated coding challenges for both PySpark and SQL. Write real code, execute instantly against live datasets, and track your progress — no setup required.
                </p>

                <div className="hero-search-bar">
                    <div className="search-input-wrapper">
                        <Search className="search-icon" size={20} />
                        <input
                            type="text"
                            placeholder="Ask the LLM anything! (e.g., How do Window Functions work?)"
                            className="search-input"
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                            onKeyDown={handleKeyDown}
                        />
                    </div>
                    {searchQuery.trim() ? (
                        <button className="search-btn" onClick={handleSearch}>Search Docs</button>
                    ) : (
                        <button className="search-btn" onClick={() => onStartPracticing('pyspark')}>Start Practicing</button>
                    )}
                </div>
            </div>

            <div className="hero-stats">
                <div className="stat-pill"><span className="stat-num">50+</span> Core Topics</div>
                <div className="stat-pill"><span className="stat-num">Infinite</span> AI Scenarios</div>
                <div className="stat-pill"><span className="stat-num">Real-time</span> Evaluation</div>
            </div>

            {/* Mode Selector Cards */}
            <div className="mode-selector-section">
                <h2 className="mode-selector-title">Choose Your Sandbox</h2>
                <div className="mode-selector-cards">
                    <div className="glass-card mode-card mode-card-pyspark" onClick={() => onStartPracticing('pyspark')}>
                        <div className="mode-card-icon">
                            <Code size={36} />
                        </div>
                        <h3>Practice PySpark</h3>
                        <p>DataFrame API, Window Functions, Joins, UDFs, Aggregations and more. Write Python code backed by a real Apache Spark engine.</p>
                        <div className="mode-card-cta">
                            Start PySpark <ArrowRight size={16} />
                        </div>
                    </div>

                    <div className="glass-card mode-card mode-card-sql" onClick={() => onStartPracticing('sql')}>
                        <div className="mode-card-icon">
                            <TerminalSquare size={36} />
                        </div>
                        <h3>Practice SQL</h3>
                        <p>SELECT, JOINs, CTEs, subqueries, window functions and more. Write standard SQL that runs on real tables powered by Spark SQL.</p>
                        <div className="mode-card-cta">
                            Start SQL <ArrowRight size={16} />
                        </div>
                    </div>
                </div>
            </div>

            {/* Feature Cards Horizontal Row */}
            <div className="features-section" id="features">
                <div className="feature-cards-container">

                    <div className="glass-card feature-card">
                        <div className="feature-icon-wrapper" style={{ color: '#a855f7', backgroundColor: 'rgba(168, 85, 247, 0.1)' }}>
                            <Sparkles size={28} />
                        </div>
                        <h3>AI Generation</h3>
                        <p>Our autonomous engine generates infinite, hyper-customized PySpark and SQL scenarios tailored to your chosen topic and skill level.</p>
                    </div>

                    <div className="glass-card feature-card">
                        <div className="feature-icon-wrapper" style={{ color: '#3b82f6', backgroundColor: 'rgba(59, 130, 246, 0.1)' }}>
                            <TerminalSquare size={28} />
                        </div>
                        <h3>Dual Sandbox</h3>
                        <p>Execute real PySpark code or run SQL queries instantly — both powered by the same Apache Spark engine, no local setup needed.</p>
                    </div>

                    <div className="glass-card feature-card">
                        <div className="feature-icon-wrapper" style={{ color: '#10b981', backgroundColor: 'rgba(16, 185, 129, 0.1)' }}>
                            <CheckCircle size={28} />
                        </div>
                        <h3>Instant Grading</h3>
                        <p>Our evaluation engine performs strict, order-agnostic row-by-row validation on both PySpark DataFrames and SQL query results.</p>
                    </div>

                    <div className="glass-card feature-card">
                        <div className="feature-icon-wrapper" style={{ color: '#ef4444', backgroundColor: 'rgba(239, 68, 68, 0.1)' }}>
                            <Flame size={28} />
                        </div>
                        <h3>Gamification</h3>
                        <p>Track your daily coding streaks via a GitHub-style heatmap, earn XP, and curate your personalized library of challenging problems.</p>
                    </div>

                </div>
            </div>

            {/* Semantic Search Modal Overlay */}
            {
                showSearchModal && (
                    <SearchResultModal
                        query={searchQuery}
                        result={searchResult}
                        isLoading={isSearching}
                        error={searchError}
                        onClose={() => {
                            setShowSearchModal(false);
                            setSearchResult(null);
                        }}
                    />
                )
            }

            {/* Profile Settings Modal */}
            {showSettingsModal && (
                <SettingsModal onClose={() => setShowSettingsModal(false)} />
            )}
        </div >
    );
};

export default LandingPage;
