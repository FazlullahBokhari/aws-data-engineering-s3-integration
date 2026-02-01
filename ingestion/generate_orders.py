import pandas as pd
from datetime import datetime, timedelta
import os

def generate_orders(num_records=10000, output_dir="data/orders/raw"):
    """
    Generate fake orders CSV locally.
    """
    # Create the directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    today = datetime.today()
    data = []

    for i in range(num_records):
        order_date = (today - timedelta(days=i % 10)).strftime("%Y-%m-%d")
        data.append({
            "order_id": i + 1,
            "customer": f"Customer_{i+1}",
            "amount": round(100 + i * 1.5, 2),
            "order_date": order_date
        })

    # Save CSV
    output_path = os.path.join(output_dir, f"orders_{today.strftime('%Y-%m-%d')}.csv")
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"Generated file: {output_path}")
    return output_path

if __name__ == "__main__":
    generate_orders()