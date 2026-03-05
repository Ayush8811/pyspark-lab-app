import React from 'react';
import ReactMarkdown from 'react-markdown';
import { Sparkles, X, Loader2 } from 'lucide-react';
import './SearchResultModal.css';

const SearchResultModal = ({ query, result, isLoading, error, onClose }) => {
    if (!query && !isLoading && !result && !error) return null;

    return (
        <div className="search-result-overlay" onClick={onClose}>
            <div className="search-result-content" onClick={e => e.stopPropagation()}>

                <div className="search-header">
                    <div className="search-title">
                        <Sparkles size={20} className="sparkle-icon" />
                        <h3>AI Explanation for "{query}"</h3>
                    </div>
                    <button className="close-btn" onClick={onClose}><X size={24} /></button>
                </div>

                <div className="search-body">
                    {isLoading ? (
                        <div className="loading-state">
                            <Loader2 size={32} className="spinner" />
                            <p>Consulting PySpark Senior Engineer...</p>
                        </div>
                    ) : error ? (
                        <div className="error-state">
                            <p>{error}</p>
                        </div>
                    ) : result ? (
                        <div className="markdown-container">
                            <ReactMarkdown>{result}</ReactMarkdown>
                        </div>
                    ) : null}
                </div>

            </div>
        </div>
    );
};

export default SearchResultModal;
