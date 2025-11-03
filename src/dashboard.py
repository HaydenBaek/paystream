import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc
from sqlalchemy import create_engine
from config.db_config import DB_CONFIG

def load_data():
    conn_str = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(conn_str)
    df = pd.read_sql("SELECT * FROM transactions", engine)
    engine.dispose()
    return df

def main():
    print("Starting dashboard...")

    df = load_data()
    total_transactions = len(df)
    total_amount = df["amount"].sum()
    fraud_rate = (df["fraud_flag"].sum() / total_transactions) * 100
    high_value_rate = (df["high_value_flag"].sum() / total_transactions) * 100

    app = Dash(__name__)
    app.title = "PayStream Analytics"

    top_amount_cities = (
        df.groupby("city", as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
        .head(15)
    )
    amount_by_city = px.bar(
        top_amount_cities,
        x="city",
        y="amount",
        title="Top 15 Cities by Transaction Volume",
        color_discrete_sequence=["#3A6EA5"]
    )
    amount_by_city.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Inter, Arial, sans-serif", size=11, color="#1e1e1e"),
        title_font=dict(size=15, color="#1e1e1e", weight=600),
        margin=dict(l=50, r=20, t=50, b=50),
        xaxis=dict(title="", tickangle=-35, tickfont=dict(size=10), showgrid=False),
        yaxis=dict(title=dict(text="Total Amount ($)", font=dict(size=11)), showgrid=True, gridcolor="#f0f0f0"),
        bargap=0.2,
        height=340
    )

    top_fraud_cities = (
        df.groupby("city", as_index=False)["fraud_flag"]
        .sum()
        .sort_values("fraud_flag", ascending=False)
        .head(15)
    )
    fraud_by_city = px.bar(
        top_fraud_cities,
        x="city",
        y="fraud_flag",
        title="Top 15 Cities by Fraud Cases",
        color_discrete_sequence=["#C44536"]
    )
    fraud_by_city.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Inter, Arial, sans-serif", size=11, color="#1e1e1e"),
        title_font=dict(size=15, color="#1e1e1e", weight=600),
        margin=dict(l=50, r=20, t=50, b=50),
        xaxis=dict(title="", tickangle=-35, tickfont=dict(size=10), showgrid=False),
        yaxis=dict(title=dict(text="Fraud Count", font=dict(size=11)), showgrid=True, gridcolor="#f0f0f0"),
        bargap=0.2,
        height=340
    )

    app.layout = html.Div(
        style={
            "fontFamily": "Inter, Arial, sans-serif",
            "backgroundColor": "#f5f7fa",
            "height": "100vh",
            "width": "100vw",
            "overflow": "hidden",
            "display": "flex",
            "flexDirection": "column",
            "padding": "15px",
            "boxSizing": "border-box",
            "position": "fixed",
            "top": "0",
            "left": "0"
        },
        children=[
            html.H2(
                "PayStream Analytics Dashboard",
                style={
                    "textAlign": "center",
                    "margin": "0 0 15px 0",
                    "color": "#1e293b",
                    "fontSize": "22px",
                    "fontWeight": "700",
                    "letterSpacing": "-0.5px",
                    "flexShrink": "0"
                }
            ),
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(4, 1fr)",
                    "gap": "12px",
                    "marginBottom": "15px",
                    "maxWidth": "1400px",
                    "margin": "0 auto 15px auto",
                    "width": "100%",
                    "flexShrink": "0"
                },
                children=[
                    html.Div(
                        [
                            html.Div("Total Transactions", style={
                                "color": "#64748b",
                                "fontSize": "12px",
                                "fontWeight": "500",
                                "marginBottom": "6px",
                                "textTransform": "uppercase",
                                "letterSpacing": "0.5px"
                            }),
                            html.Div(f"{total_transactions:,}", style={
                                "color": "#0f172a",
                                "fontSize": "26px",
                                "fontWeight": "700",
                                "lineHeight": "1"
                            })
                        ],
                        style={
                            "backgroundColor": "white",
                            "padding": "16px",
                            "borderRadius": "12px",
                            "boxShadow": "0 1px 3px rgba(0,0,0,0.06)",
                            "border": "1px solid #e2e8f0"
                        }
                    ),
                    html.Div(
                        [
                            html.Div("Total Amount", style={
                                "color": "#64748b",
                                "fontSize": "12px",
                                "fontWeight": "500",
                                "marginBottom": "6px",
                                "textTransform": "uppercase",
                                "letterSpacing": "0.5px"
                            }),
                            html.Div(f"${total_amount:,.0f}", style={
                                "color": "#059669",
                                "fontSize": "26px",
                                "fontWeight": "700",
                                "lineHeight": "1"
                            })
                        ],
                        style={
                            "backgroundColor": "white",
                            "padding": "20px",
                            "borderRadius": "12px",
                            "boxShadow": "0 1px 3px rgba(0,0,0,0.06)",
                            "border": "1px solid #e2e8f0"
                        }
                    ),
                    html.Div(
                        [
                            html.Div("Fraud Rate", style={
                                "color": "#64748b",
                                "fontSize": "12px",
                                "fontWeight": "500",
                                "marginBottom": "6px",
                                "textTransform": "uppercase",
                                "letterSpacing": "0.5px"
                            }),
                            html.Div(f"{fraud_rate:.2f}%", style={
                                "color": "#dc2626",
                                "fontSize": "26px",
                                "fontWeight": "700",
                                "lineHeight": "1"
                            })
                        ],
                        style={
                            "backgroundColor": "white",
                            "padding": "20px",
                            "borderRadius": "12px",
                            "boxShadow": "0 1px 3px rgba(0,0,0,0.06)",
                            "border": "1px solid #e2e8f0"
                        }
                    ),
                    html.Div(
                        [
                            html.Div("High-Value Rate", style={
                                "color": "#64748b",
                                "fontSize": "12px",
                                "fontWeight": "500",
                                "marginBottom": "6px",
                                "textTransform": "uppercase",
                                "letterSpacing": "0.5px"
                            }),
                            html.Div(f"{high_value_rate:.2f}%", style={
                                "color": "#2563eb",
                                "fontSize": "26px",
                                "fontWeight": "700",
                                "lineHeight": "1"
                            })
                        ],
                        style={
                            "backgroundColor": "white",
                            "padding": "20px",
                            "borderRadius": "12px",
                            "boxShadow": "0 1px 3px rgba(0,0,0,0.06)",
                            "border": "1px solid #e2e8f0"
                        }
                    ),
                ]
            ),
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "1fr 1fr",
                    "gap": "16px",
                    "flex": "1",
                    "minHeight": "0",
                    "maxWidth": "1400px",
                    "margin": "0 auto",
                    "width": "100%"
                },
                children=[
                    html.Div(
                        dcc.Graph(
                            figure=amount_by_city,
                            style={"height": "100%"},
                            config={'displayModeBar': False}
                        ),
                        style={
                            "backgroundColor": "white",
                            "borderRadius": "12px",
                            "boxShadow": "0 1px 3px rgba(0,0,0,0.06)",
                            "border": "1px solid #e2e8f0",
                            "padding": "16px",
                            "minHeight": "0"
                        }
                    ),
                    html.Div(
                        dcc.Graph(
                            figure=fraud_by_city,
                            style={"height": "100%"},
                            config={'displayModeBar': False}
                        ),
                        style={
                            "backgroundColor": "white",
                            "borderRadius": "12px",
                            "boxShadow": "0 1px 3px rgba(0,0,0,0.06)",
                            "border": "1px solid #e2e8f0",
                            "padding": "16px",
                            "minHeight": "0"
                        }
                    )
                ]
            )
        ]
    )

    app.run(debug=True, port=8050)

if __name__ == "__main__":
    main()