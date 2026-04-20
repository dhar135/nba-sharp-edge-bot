# src/fix_voids.py
import sqlite3

DB_NAME = "sharp_edge.db"

def revert_voids():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Update any mistakenly voided bets back to PENDING
    cursor.execute('''
        UPDATE predictions 
        SET status = 'PENDING', actual_result = 0.0 
        WHERE status = 'VOID (DNP)'
    ''')
    
    rows_updated = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"[+] Successfully reverted {rows_updated} 'VOID (DNP)' bets back to 'PENDING'.")
    print("[*] You can now run `python src/grader.py` to grade them correctly!")

if __name__ == "__main__":
    revert_voids()