import React, { useState, useEffect, useContext } from 'react';
import axios from 'axios';
import { Shield, Target, Flame, Trophy, Loader2, Calendar, FileText, ChevronRight, PieChart } from 'lucide-react';
import { AuthContext } from './AuthContext';
import { API_BASE_URL } from './config';
import './ProfileDashboard.css';

const ProfileDashboard = ({ onClose }) => {
    const { user, token } = useContext(AuthContext);
    const [profileData, setProfileData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');

    const [activeTab, setActiveTab] = useState('Overview');

    useEffect(() => {
        const fetchProfile = async () => {
            try {
                const res = await axios.get(`${API_BASE_URL}/api/user/profile`, {
                    headers: { Authorization: `Bearer ${token}` }
                });
                setProfileData(res.data);
            } catch (err) {
                console.error(err);
                setError('Failed to load profile data.');
            } finally {
                setLoading(false);
            }
        };

        if (user && token) {
            fetchProfile();
        }
    }, [user, token]);

    const renderHeatmap = () => {
        if (!profileData?.activity_heatmap) return null;

        const heatmapData = profileData.activity_heatmap;
        const today = new Date();
        const days = [];

        // Generate exactly 365 blocks backwards
        for (let i = 364; i >= 0; i--) {
            const d = new Date(today);
            d.setDate(d.getDate() - i);
            const dateStr = d.toISOString().split('T')[0];
            const count = heatmapData[dateStr] || 0;
            days.push({ date: dateStr, count });
        }

        const getIntensity = (count) => {
            if (count === 0) return 'level-0';
            if (count >= 1 && count <= 2) return 'level-1';
            if (count >= 3 && count <= 4) return 'level-2';
            if (count >= 5 && count <= 6) return 'level-3';
            return 'level-4';
        };

        return (
            <div className="heatmap-container">
                <div className="heatmap-grid">
                    {days.map((day, i) => (
                        <div
                            key={i}
                            className={`heatmap-cell ${getIntensity(day.count)}`}
                            title={`${day.count} submissons on ${day.date}`}
                        />
                    ))}
                </div>
            </div>
        );
    };

    const getDifficultyBadge = (difficulty) => {
        const cls = `badge-${difficulty.toLowerCase()}`;
        return <span className={`status-badge ${cls}`}>{difficulty}</span>;
    };

    return (
        <div className="dashboard-overlay" onClick={onClose}>
            <div className="dashboard-content" onClick={e => e.stopPropagation()}>

                {/* Sidebar Navigation */}
                <div className="dashboard-sidebar">
                    <div className="dashboard-header">
                        <div className="avatar-large">
                            <Shield size={32} />
                        </div>
                        <h2>{user?.username}</h2>
                        <span className="profile-role">PySpark Student</span>
                    </div>

                    <div className="dashboard-nav">
                        <button
                            className={`nav-btn ${activeTab === 'Overview' ? 'active' : ''}`}
                            onClick={() => setActiveTab('Overview')}
                        >
                            <Target size={18} /> Overview
                        </button>
                        <button
                            className={`nav-btn ${activeTab === 'History' ? 'active' : ''}`}
                            onClick={() => setActiveTab('History')}
                        >
                            <FileText size={18} /> Submission History
                        </button>
                        <button
                            className={`nav-btn ${activeTab === 'Analytics' ? 'active' : ''}`}
                            onClick={() => setActiveTab('Analytics')}
                        >
                            <PieChart size={18} /> Analytics
                        </button>
                    </div>

                    <button className="close-dashboard-btn" onClick={onClose}>
                        Back to Editor
                    </button>
                </div>

                {/* Main Content Pane */}
                <div className="dashboard-main">
                    {loading ? (
                        <div className="loader-container">
                            <Loader2 size={32} className="spinner" />
                        </div>
                    ) : error ? (
                        <div className="error-text">{error}</div>
                    ) : (
                        <div className="dashboard-scrollable-area">

                            {/* === OVERVIEW TAB === */}
                            {activeTab === 'Overview' && (
                                <>
                                    <h1 className="tab-title">Overview</h1>

                                    <div className="kpi-cards">
                                        <div className="kpi-card">
                                            <div className="kpi-icon-wrapper" style={{ backgroundColor: 'rgba(59, 130, 246, 0.1)', color: '#3b82f6' }}>
                                                <Target size={24} />
                                            </div>
                                            <div className="kpi-text">
                                                <span className="kpi-label">Total Solved</span>
                                                <span className="kpi-value">{profileData.stats.total_solved}</span>
                                                <span className="kpi-subtext">Across all difficulties</span>
                                            </div>
                                        </div>

                                        <div className="kpi-card">
                                            <div className="kpi-icon-wrapper" style={{ backgroundColor: 'rgba(251, 191, 36, 0.1)', color: '#fbbf24' }}>
                                                <Trophy size={24} />
                                            </div>
                                            <div className="kpi-text">
                                                <span className="kpi-label">XP Score</span>
                                                <span className="kpi-value">{profileData.stats.xp}</span>
                                                <span className="kpi-subtext">Top 15% of users</span>
                                            </div>
                                        </div>

                                        <div className="kpi-card">
                                            <div className="kpi-icon-wrapper" style={{ backgroundColor: 'rgba(239, 68, 68, 0.1)', color: '#ef4444' }}>
                                                <Flame size={24} />
                                            </div>
                                            <div className="kpi-text">
                                                <span className="kpi-label">Day Streak</span>
                                                <span className="kpi-value">{profileData.stats.current_streak}</span>
                                                <span className="kpi-subtext">Keep it up! 🔥</span>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="dashboard-section heatmap-section">
                                        <h3>365 Days of Code</h3>
                                        <p className="section-desc">Your daily contribution matrix tracks your successful problem submissions over the last year.</p>
                                        {renderHeatmap()}
                                        <div className="heatmap-legend">
                                            <span>Less</span>
                                            <div className="heatmap-cell level-0"></div>
                                            <div className="heatmap-cell level-1"></div>
                                            <div className="heatmap-cell level-2"></div>
                                            <div className="heatmap-cell level-3"></div>
                                            <div className="heatmap-cell level-4"></div>
                                            <span>More</span>
                                        </div>
                                    </div>
                                </>
                            )}

                            {/* === HISTORY TAB === */}
                            {activeTab === 'History' && (
                                <>
                                    <h1 className="tab-title">Submission History</h1>
                                    <p className="section-desc">A detailed log of all your successfully evaluated problems.</p>

                                    <div className="dashboard-section table-section">
                                        <table className="history-table">
                                            <thead>
                                                <tr>
                                                    <th>Problem Title</th>
                                                    <th>Difficulty</th>
                                                    <th>Date Solved</th>
                                                    <th>Status</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {profileData.recent_submissions && profileData.recent_submissions.length > 0 ? (
                                                    profileData.recent_submissions.map((sub, idx) => (
                                                        <tr key={idx}>
                                                            <td className="problem-title-cell">{sub.problem_title || `Legacy Problem #${sub.id}`}</td>
                                                            <td>{getDifficultyBadge(sub.difficulty)}</td>
                                                            <td className="date-cell">
                                                                <Calendar size={14} style={{ marginRight: '6px' }} />
                                                                {sub.date}
                                                            </td>
                                                            <td>
                                                                <span className="status-badge badge-passed">Passed</span>
                                                            </td>
                                                        </tr>
                                                    ))
                                                ) : (
                                                    <tr>
                                                        <td colSpan="4" style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-muted)' }}>
                                                            No submissions yet. Go solve some PySpark problems!
                                                        </td>
                                                    </tr>
                                                )}
                                            </tbody>
                                        </table>
                                    </div>
                                </>
                            )}

                            {/* === ANALYTICS TAB === */}
                            {activeTab === 'Analytics' && (
                                <>
                                    <h1 className="tab-title">Complexity Breakdown</h1>
                                    <p className="section-desc">Analyze the distribution of problem difficulties you've successfully conquered.</p>

                                    <div className="dashboard-section analytics-section">
                                        <div className="complexity-breakdown-dash">
                                            <div className="progress-bars-dash">
                                                <div className="bar-wrapper-dash">
                                                    <span className="diff-label">Easy</span>
                                                    <div className="bar-bg-dash">
                                                        <div className="bar-fill-dash bg-success" style={{ width: `${Math.min(100, (profileData.stats.by_complexity.Easy || 0) * 5)}%` }}></div>
                                                    </div>
                                                    <span className="count-label-dash">{profileData.stats.by_complexity.Easy || 0}</span>
                                                </div>
                                                <div className="bar-wrapper-dash">
                                                    <span className="diff-label">Medium</span>
                                                    <div className="bar-bg-dash">
                                                        <div className="bar-fill-dash bg-warning" style={{ width: `${Math.min(100, (profileData.stats.by_complexity.Medium || 0) * 5)}%` }}></div>
                                                    </div>
                                                    <span className="count-label-dash">{profileData.stats.by_complexity.Medium || 0}</span>
                                                </div>
                                                <div className="bar-wrapper-dash">
                                                    <span className="diff-label">Hard</span>
                                                    <div className="bar-bg-dash">
                                                        <div className="bar-fill-dash bg-error" style={{ width: `${Math.min(100, (profileData.stats.by_complexity.Hard || 0) * 5)}%` }}></div>
                                                    </div>
                                                    <span className="count-label-dash">{profileData.stats.by_complexity.Hard || 0}</span>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                </>
                            )}

                        </div>
                    )}
                </div>

            </div>
        </div>
    );
};

export default ProfileDashboard;
