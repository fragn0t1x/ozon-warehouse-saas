from app.services.price_risk_alerts_service import PriceRiskAlertsService


def _row(**overrides):
    base = {
        "current_profitability_available": True,
        "unit_cost": 100.0,
        "current_estimated_net_profit": 100.0,
        "current_margin_ratio": 0.2,
    }
    base.update(overrides)
    return base


def test_classify_row_skips_rows_without_cost():
    assert PriceRiskAlertsService.classify_row(_row(unit_cost=0)) == "none"


def test_classify_row_marks_low_margin():
    assert PriceRiskAlertsService.classify_row(_row(current_estimated_net_profit=25, current_margin_ratio=0.03)) == "low_margin"


def test_classify_row_marks_break_even():
    assert PriceRiskAlertsService.classify_row(_row(current_estimated_net_profit=5, current_margin_ratio=0.02)) == "break_even"


def test_classify_row_marks_loss():
    assert PriceRiskAlertsService.classify_row(_row(current_estimated_net_profit=-10, current_margin_ratio=-0.01)) == "loss"


def test_classify_row_marks_critical_loss():
    assert PriceRiskAlertsService.classify_row(_row(current_estimated_net_profit=-80, current_margin_ratio=-0.1)) == "critical_loss"
