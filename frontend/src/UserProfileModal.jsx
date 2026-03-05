import React, { useState, useEffect, useContext } from 'react';
import axios from 'axios';
import { Shield, Target, Flame, Trophy, Loader2 } from 'lucide-react';
import { AuthContext } from './AuthContext';
import './UserProfileModal.css';

const UserProfileModal = ({ onClose }) => {
    const { user, token } = useContext(AuthContext);
    const [profileData, setProfileData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');

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

    // Generate 365 Days Array for Heatmap Grid
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

        // Optional: Determine intensity
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

    return (
        <div className="profile-modal-overlay" onClick={onClose}>
            <div className="profile-modal-content" onClick={e => e.stopPropagation()}>
                <button className="close-btn" onClick={onClose}>&times;</button>

                {loading ? (
                    <div className="loader-container">
                        <Loader2 size={32} className="spinner" />
                    </div>
                ) : error ? (
                    <div className="error-text">{error}</div>
                ) : (
                    <>
                        <div className="profile-header">
                            <div className="avatar-large">
                                <Shield size={32} />
                            </div>
                            <div>
                                <h2>{profileData.username}'s Dashboard</h2>
                                <div className="profile-role">PySpark Student</div>
                            </div>
                        </div>

                        <div className="stats-grid">
                            <div className="stat-card">
                                <Target className="stat-icon" style={{ color: 'var(--accent)' }} />
                                <div className="stat-value">{profileData.stats.total_solved}</div>
                                <div className="stat-label">Problems Solved</div>
                            </div>
                            <div className="stat-card">
                                <Trophy className="stat-icon" style={{ color: '#fbbf24' }} />
                                <div className="stat-value">{profileData.stats.xp}</div>
                                <div className="stat-label">Total XP Score</div>
                            </div>
                            <div className="stat-card">
                                <Flame className="stat-icon" style={{ color: '#ef4444' }} />
                                <div className="stat-value">{profileData.stats.current_streak}</div>
                                <div className="stat-label">Day Streak</div>
                            </div>
                        </div>

                        <div className="complexity-breakdown">
                            <h3>Problem Complexity</h3>
                            <div className="progress-bars">
                                <div className="bar-wrapper">
                                    <span>Easy</span>
                                    <div className="bar-bg">
                                        <div className="bar-fill bg-success" style={{ width: `${Math.min(100, (profileData.stats.by_complexity.Easy || 0) * 10)}%` }}></div>
                                    </div>
                                    <span className="count-label">{profileData.stats.by_complexity.Easy || 0}</span>
                                </div>
                                <div className="bar-wrapper">
                                    <span>Medium</span>
                                    <div className="bar-bg">
                                        <div className="bar-fill bg-warning" style={{ width: `${Math.min(100, (profileData.stats.by_complexity.Medium || 0) * 10)}%` }}></div>
                                    </div>
                                    <span className="count-label">{profileData.stats.by_complexity.Medium || 0}</span>
                                </div>
                                <div className="bar-wrapper">
                                    <span>Hard</span>
                                    <div className="bar-bg">
                                        <div className="bar-fill bg-error" style={{ width: `${Math.min(100, (profileData.stats.by_complexity.Hard || 0) * 10)}%` }}></div>
                                    </div>
                                    <span className="count-label">{profileData.stats.by_complexity.Hard || 0}</span>
                                </div>
                            </div>
                        </div>

                        <div className="activity-section">
                            <h3>365 Days of Code</h3>
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
            </div>
        </div>
    );
};

export default UserProfileModal;
