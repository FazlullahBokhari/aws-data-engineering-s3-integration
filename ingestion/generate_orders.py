import pandas as pd
from datetime import datetime, timedelta
import os

def generate_orders(num_records=10000, output_dir="data/orders/raw"):
    """
    Generate fake orders CSV locally.
    """
    try:
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)

        today = datetime.today()
        data = []

        print(f"Generating {num_records} order records...")

        for i in range(num_records):
            order_date = (today - timedelta(days=i % 10)).strftime("%Y-%m-%d")
            data.append({
                "order_id": i + 1,
                "customer": f"Customer_{i + 1}",
                "amount": round(100 + i * 1.5, 2),
                "order_date": order_date
            })

        # Save CSV
        output_path = os.path.join(output_dir, f"orders_{today.strftime('%Y-%m-%d')}.csv")
        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False)

        # Print summary
        print(f"✓ Generated file: {output_path}")
        print(f"✓ File size: {os.path.getsize(output_path) / 1024:.2f} KB")
        print(f"✓ Date range: {df['order_date'].min()} to {df['order_date'].max()}")
        print(f"✓ Amount range: ${df['amount'].min():.2f} to ${df['amount'].max():.2f}")

        return output_path

    except Exception as e:
        print(f"✗ Error generating orders: {e}")
        return None


if __name__ == "__main__":
    # Parse command line arguments
    import argparse

    parser = argparse.ArgumentParser(description='Generate fake order data')
    parser.add_argument('--num_records', type=int, default=10000,
                        help='Number of records to generate (default: 10000)')
    parser.add_argument('--output_dir', type=str, default="data/orders/raw",
                        help='Output directory (default: data/orders/raw)')

    args = parser.parse_args()

    generate_orders(num_records=args.num_records, output_dir=args.output_dir)