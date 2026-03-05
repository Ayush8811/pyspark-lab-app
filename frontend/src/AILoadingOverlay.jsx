import React, { useState, useEffect } from 'react';
import { Cpu } from 'lucide-react';
import './AILoadingOverlay.css';

const AILoadingOverlay = ({ isVisible, difficulty }) => {
    const [textIndex, setTextIndex] = useState(0);

    const phases = [
        "Initializing neural matrix...",
        `Analyzing ${difficulty} PySpark constraints...`,
        "Parsing dataframe relational models...",
        "Synthesizing customized edge cases...",
        "Compiling autonomous coding challenge..."
    ];

    useEffect(() => {
        if (!isVisible) return;

        setTextIndex(0);

        // Cycle text phases every 1.5 seconds to build anticipation
        const interval = setInterval(() => {
            setTextIndex((prev) => (prev < phases.length - 1 ? prev + 1 : prev));
        }, 1500);

        return () => clearInterval(interval);
    }, [isVisible, difficulty]);

    if (!isVisible) return null;

    return (
        <div className="ai-loading-overlay">
            <div className="scanline"></div>

            <div className="ai-loading-content">
                <div className="icon-container">
                    <div className="pulse-ring"></div>
                    <Cpu size={64} className="cpu-icon" />
                </div>

                <h2 className="ai-loading-title">Generating Environment</h2>

                <div className="terminal-container">
                    <div className="terminal-header">
                        <span className="dot red"></span>
                        <span className="dot yellow"></span>
                        <span className="dot green"></span>
                        <span className="terminal-title">bash -- agent-protocol</span>
                    </div>
                    <div className="terminal-body">
                        <span className="prompt">root@px-platform:~#</span>
                        <span className="typing-text">{phases[textIndex]}</span>
                        <span className="cursor">_</span>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default AILoadingOverlay;
