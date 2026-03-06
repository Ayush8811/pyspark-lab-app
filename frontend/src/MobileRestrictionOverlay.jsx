import React from 'react';
import { Monitor, Laptop, ArrowLeft } from 'lucide-react';
import './MobileRestrictionOverlay.css';

const MobileRestrictionOverlay = ({ onGoBack }) => {
  return (
    <div className="mobile-restriction-overlay">
      <div className="space-background">
        <div className="glow-orb primary"></div>
        <div className="glow-orb secondary"></div>
      </div>

      <div className="restriction-card">
        <div className="restriction-icon-wrapper">
          <Monitor size={52} />
        </div>

        <h1 className="restriction-title">Desktop Required</h1>

        <p className="restriction-message">
          The PySpark coding sandbox needs a laptop or desktop screen to work properly.
          The code editor, split-panel IDE, and execution environment are built for larger screens.
        </p>

        <div className="restriction-devices">
          <div className="device-item">
            <Laptop size={28} />
            <span>Laptop</span>
          </div>
          <div className="device-divider">or</div>
          <div className="device-item">
            <Monitor size={28} />
            <span>Desktop</span>
          </div>
        </div>

        <p className="restriction-hint">
          You can still browse the landing page, search the docs, and manage your profile on mobile.
        </p>

        <button className="restriction-back-btn" onClick={onGoBack}>
          <ArrowLeft size={18} />
          Back to Home
        </button>
      </div>
    </div>
  );
};

export default MobileRestrictionOverlay;
