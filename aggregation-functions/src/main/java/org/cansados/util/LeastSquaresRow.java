package org.cansados.util;

import java.io.Serializable;

public class LeastSquaresRow implements Serializable {

    public LeastSquaresRow(Double meanTemp, Double meanSeaLevelPressure, Double meanWindspeed, Double meanVisibility) {
        this.meanTemp = meanTemp;
        this.meanSeaLevelPressure = meanSeaLevelPressure;
        this.meanWindspeed = meanWindspeed;
        this.meanVisibility = meanVisibility;
    }

    private Double meanTemp;

    private Double meanSeaLevelPressure;

    private Double meanWindspeed;

    private Double meanVisibility;

    public Double getMeanTemp() {
        return meanTemp;
    }

    public void setMeanTemp(Double meanTemp) {
        this.meanTemp = meanTemp;
    }

    public Double getMeanSeaLevelPressure() {
        return meanSeaLevelPressure;
    }

    public void setMeanSeaLevelPressure(Double meanSeaLevelPressure) {
        this.meanSeaLevelPressure = meanSeaLevelPressure;
    }

    public Double getMeanWindspeed() {
        return meanWindspeed;
    }

    public void setMeanWindspeed(Double meanWindspeed) {
        this.meanWindspeed = meanWindspeed;
    }

    public Double getMeanVisibility() {
        return meanVisibility;
    }

    public void setMeanVisibility(Double meanVisibility) {
        this.meanVisibility = meanVisibility;
    }
}
