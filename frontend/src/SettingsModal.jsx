import React, { useState, useContext, useEffect } from 'react';
import axios from 'axios';
import { X, Save, Loader2 } from 'lucide-react';
import { AuthContext } from './AuthContext';
import { API_BASE_URL } from './config';
import './SettingsModal.css';

const SettingsModal = ({ onClose }) => {
    const { token, user, checkSession } = useContext(AuthContext);

    const [formData, setFormData] = useState({
        name: '',
        age: '',
        bio: ''
    });

    const [isSaving, setIsSaving] = useState(false);
    const [error, setError] = useState(null);
    const [successMessage, setSuccessMessage] = useState(null);

    // Pre-fill form with existing user data
    useEffect(() => {
        if (user) {
            setFormData({
                name: user.name || '',
                age: user.age || '',
                bio: user.bio || ''
            });
        }
    }, [user]);

    const handleSave = async (e) => {
        e.preventDefault();
        setIsSaving(true);
        setError(null);
        setSuccessMessage(null);

        try {
            const payload = {
                name: formData.name.trim() || null,
                age: formData.age !== '' ? parseInt(formData.age, 10) : null,
                bio: formData.bio.trim() || null
            };

            await axios.put(`${API_BASE_URL}/api/user/settings`, payload, {
                headers: {
                    Authorization: `Bearer ${token}`
                }
            });

            setSuccessMessage("Profile updated successfully!");
            if (checkSession) await checkSession(); // Refresh context to match new data globally
        } catch (err) {
            console.error(err);
            setError("Failed to update profile. Please try again.");
        } finally {
            setIsSaving(false);
        }
    };

    return (
        <div className="settings-modal-overlay" onClick={onClose}>
            <div className="settings-modal-content" onClick={(e) => e.stopPropagation()}>

                <div className="settings-header">
                    <h2>Edit Profile Metadata</h2>
                    <button className="close-btn" onClick={onClose}><X size={20} /></button>
                </div>

                <form className="settings-body" onSubmit={handleSave}>
                    {error && <div className="settings-error">{error}</div>}
                    {successMessage && <div className="settings-success">{successMessage}</div>}

                    <div className="input-group">
                        <label>Display Name</label>
                        <input
                            type="text"
                            placeholder="Captain PySpark..."
                            value={formData.name}
                            onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                        />
                    </div>

                    <div className="input-group">
                        <label>Age</label>
                        <input
                            type="number"
                            placeholder="Optional"
                            min="0"
                            max="120"
                            value={formData.age}
                            onChange={(e) => setFormData({ ...formData, age: e.target.value })}
                        />
                    </div>

                    <div className="input-group">
                        <label>Developer Bio</label>
                        <textarea
                            placeholder="Tell us about your data engineering journey..."
                            rows={3}
                            value={formData.bio}
                            onChange={(e) => setFormData({ ...formData, bio: e.target.value })}
                            maxLength={160}
                        />
                        <span className="char-count">{formData.bio.length} / 160</span>
                    </div>

                    <div className="settings-footer">
                        <button type="button" className="glass-btn text-only" onClick={onClose}>Cancel</button>
                        <button type="submit" className="glass-btn primary-gradient" disabled={isSaving}>
                            {isSaving ? <Loader2 size={16} className="spinner" /> : <Save size={16} />}
                            {isSaving ? 'Saving...' : 'Save Settings'}
                        </button>
                    </div>
                </form>
            </div>
        </div>
    );
};

export default SettingsModal;
