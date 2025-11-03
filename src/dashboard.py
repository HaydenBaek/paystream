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

    # Show only top 15 cities by amount
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
        font=dict(family="Inter, Arial, sans-serif", size=13, color="#1e1e1e"),
        title_font=dict(size=18, color="#1e1e1e"),
        margin=dict(l=40, r=40, t=60, b=80),
        xaxis=dict(
            title="City",
            tickangle=-30,
            tickfont=dict(size=11),
            showgrid=False,
            categoryorder="total descending"
        ),
        yaxis=dict(title="Total Amount ($)", showgrid=True, gridcolor="#eaeaea"),
        bargap=0.25
    )

    # Top 15 cities by fraud count
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
        font=dict(family="Inter, Arial, sans-serif", size=13, color="#1e1e1e"),
        title_font=dict(size=18, color="#1e1e1e"),
        margin=dict(l=40, r=40, t=60, b=80),
        xaxis=dict(
            title="City",
            tickangle=-30,
            tickfont=dict(size=11),
            showgrid=False,
            categoryorder="total descending"
        ),
        yaxis=dict(title="Fraud Count", showgrid=True, gridcolor="#eaeaea"),
        bargap=0.25
    )

    app.layout = html.Div(
        style={
            "fontFamily": "Inter, Arial, sans-serif",
            "backgroundColor": "#F7F8FA",
            "padding": "50px",
            "minHeight": "100vh"
        },
        children=[
            html.H1(
                "PayStream Analytics Dashboard",
                style={
                    "textAlign": "center",
                    "marginBottom": "50px",
                    "color": "#1e1e1e",
                    "fontSize": "36px",
                    "fontWeight": "600",
                    "letterSpacing": "-0.5px"
                }
            ),
            html.Div(
                style={
                    "display": "flex",
                    "justifyContent": "space-evenly",
                    "marginBottom": "50px",
                    "flexWrap": "wrap",
                    "gap": "20px"
                },
                children=[
                    html.Div(
                        [
                            html.H4("Total Transactions", style={"color": "#6b7280", "marginBottom": "8px"}),
                            html.H2(f"{total_transactions:,}", style={"color": "#111827", "fontSize": "28px"})
                        ],
                        style={
                            "backgroundColor": "white",
                            "padding": "25px",
                            "borderRadius": "10px",
                            "width": "22%",
                            "minWidth": "220px",
                            "boxShadow": "0 2px 6px rgba(0,0,0,0.08)",
                            "textAlign": "center"
                        }
                    ),
                    html.Div(
                        [
                            html.H4("Total Transaction Amount", style={"color": "#6b7280", "marginBottom": "8px"}),
                            html.H2(f"${total_amount:,.2f}", style={"color": "#16a34a", "fontSize": "28px"})
                        ],
                        style={
                            "backgroundColor": "white",
                            "padding": "25px",
                            "borderRadius": "10px",
                            "width": "22%",
                            "minWidth": "220px",
                            "boxShadow": "0 2px 6px rgba(0,0,0,0.08)",
                            "textAlign": "center"
                        }
                    ),
                    html.Div(
                        [
                            html.H4("Fraud Rate", style={"color": "#6b7280", "marginBottom": "8px"}),
                            html.H2(f"{fraud_rate:.2f}%", style={"color": "#dc2626", "fontSize": "28px"})
                        ],
                        style={
                            "backgroundColor": "white",
                            "padding": "25px",
                            "borderRadius": "10px",
                            "width": "22%",
                            "minWidth": "220px",
                            "boxShadow": "0 2px 6px rgba(0,0,0,0.08)",
                            "textAlign": "center"
                        }
                    ),
                    html.Div(
                        [
                            html.H4("High-Value Transaction Rate", style={"color": "#6b7280", "marginBottom": "8px"}),
                            html.H2(f"{high_value_rate:.2f}%", style={"color": "#2563eb", "fontSize": "28px"})
                        ],
                        style={
                            "backgroundColor": "white",
                            "padding": "25px",
                            "borderRadius": "10px",
                            "width": "22%",
                            "minWidth": "220px",
                            "boxShadow": "0 2px 6px rgba(0,0,0,0.08)",
                            "textAlign": "center"
                        }
                    ),
                ]
            ),
            html.Div(
                [
                    dcc.Graph(figure=amount_by_city, style={"marginBottom": "60px"}),
                    dcc.Graph(figure=fraud_by_city)
                ],
                style={
                    "backgroundColor": "white",
                    "padding": "35px",
                    "borderRadius": "12px",
                    "boxShadow": "0 4px 10px rgba(0,0,0,0.08)"
                }
            )
        ]
    )

    app.run(debug=True, port=8050)

if __name__ == "__main__":
    main()
