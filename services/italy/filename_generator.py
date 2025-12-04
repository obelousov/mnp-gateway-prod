"""
Filename Generator for Italy MNP XML files
Naming convention: ****YYYYMMGGhhmmss++++nnnnn
Where: **** = sender operator code, ++++ = recipient operator code
Example: PMOB20251020120055LMIT99137
"""

from datetime import datetime
from typing import Optional
import os
import json

class FilenameGenerator:
    """Generate filenames according to Italy MNP specifications"""
    
    @staticmethod
    def generate_mnp_filename(
        sender_operator_code: str,
        recipient_operator_code: str,
        file_id: str,
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Generate filename according to Italy MNP specification:
        ****YYYYMMGGhhmmss++++nnnnn
        
        Where:
        - ****: 4-character sender operator code
        - YYYY: 4-digit year
        - MM: 2-digit month
        - GG: 2-digit day
        - hhmmss: 6-digit hour, minute, second
        - ++++: 4-character recipient operator code
        - nnnnn: 5-digit progressive number (file_id)
        
        Example: PMOB20251020120055LMIT99137
        
        Args:
            sender_operator_code: 4-character sender operator code (e.g., "PMOB")
            recipient_operator_code: 4-character recipient operator code (e.g., "LMIT")
            file_id: 5-digit progressive number (e.g., "99137")
            timestamp: Optional datetime, defaults to current time
        
        Returns:
            Formatted filename string
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        # Format date and time components
        date_part = timestamp.strftime("%Y%m%d")  # YYYYMMGG
        time_part = timestamp.strftime("%H%M%S")  # hhmmss
        
        # Ensure sender code is 4 characters (pad if needed)
        sender_part = sender_operator_code.strip().upper().ljust(4, '_')
        if len(sender_part) > 4:
            sender_part = sender_part[:4]
        
        # Ensure recipient code is 4 characters (pad if needed)
        recipient_part = recipient_operator_code.strip().upper().ljust(4, '_')
        if len(recipient_part) > 4:
            recipient_part = recipient_part[:4]
        
        # Ensure file_id is 5 digits (pad with zeros)
        file_id_part = str(file_id).zfill(5)
        if len(file_id_part) > 5:
            file_id_part = file_id_part[:5]
        
        # Construct filename: sender + date + time + recipient + file_id
        filename = f"{sender_part}{date_part}{time_part}{recipient_part}{file_id_part}"
        
        return filename
    
    @staticmethod
    def generate_xml_filename(
        sender_operator: str,
        recipient_operator: str,
        file_id: str,
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Generate full XML filename with .xml extension
        
        Example: PMOB_LMIT_20251020120055_99137.xml
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        timestamp_str = timestamp.strftime("%Y%m%d%H%M%S")
        
        # Simple descriptive filename (also acceptable)
        filename = f"{sender_operator}_{recipient_operator}_{timestamp_str}_{file_id}.xml"
        
        return filename
    
    @staticmethod
    def generate_ack_filename(
        sender_operator: str,
        recipient_operator: str,
        original_filename: str,
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Generate acknowledgment filename
        
        Format: ACK_<sender>_<recipient>_<timestamp>.xml
        Example: ACK_LMIT_PMOB_20251020120055.xml
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        timestamp_str = timestamp.strftime("%Y%m%d%H%M%S")
        filename = f"ACK_{sender_operator}_{recipient_operator}_{timestamp_str}.xml"
        
        return filename
    
    @staticmethod
    def parse_mnp_filename(filename: str) -> dict:
        """
        Parse MNP filename into its components
        
        Args:
            filename: ****YYYYMMGGhhmmss++++nnnnn format
        
        Returns:
            Dictionary with parsed components
        """
        if len(filename) != 27:  # 4 + 8 + 6 + 4 + 5 = 27 characters total
            raise ValueError(f"Invalid filename length. Expected 27 chars, got {len(filename)}")
        
        # Parse components
        sender_part = filename[:4]  # ****
        date_part = filename[4:12]  # YYYYMMGG
        time_part = filename[12:18]  # hhmmss
        recipient_part = filename[18:22]  # ++++
        file_id_part = filename[22:]  # nnnnn
        
        # Parse date and time
        try:
            file_date = datetime.strptime(f"{date_part}{time_part}", "%Y%m%d%H%M%S")
        except ValueError as e:
            raise ValueError(f"Invalid date/time in filename: {e}")
        
        return {
            "original_filename": filename,
            "sender_operator": sender_part.strip('_'),
            "recipient_operator": recipient_part.strip('_'),
            "file_date": file_date.date(),
            "file_time": file_date.time(),
            "year": date_part[:4],
            "month": date_part[4:6],
            "day": date_part[6:8],
            "hour": time_part[:2],
            "minute": time_part[2:4],
            "second": time_part[4:6],
            "file_id": file_id_part.lstrip('0') or "0",
            "formatted_file_id": file_id_part,
        }
    
    @staticmethod
    def generate_daily_sequence_number(
        sender_operator: str,
        recipient_operator: str,
        date: Optional[datetime] = None
    ) -> str:
        """
        Generate daily sequence number for file_id
        Resets to 00001 each day for each sender-recipient pair
        
        Args:
            sender_operator: Sender operator code
            recipient_operator: Recipient operator code
            date: Date for sequence (defaults to today)
        
        Returns:
            5-digit sequence number as string
        """
        if date is None:
            date = datetime.now()
        
        date_str = date.strftime("%Y%m%d")
        sequence_key = f"{sender_operator}_{recipient_operator}_{date_str}"
        
        try:
            # File-based counter (in production, use database)
            counter_file = "/tmp/mnp_daily_counter.json"
            
            if os.path.exists(counter_file):
                with open(counter_file, 'r') as f:
                    data = json.load(f)
            else:
                data = {}
            
            # Reset counter for new day or new pair
            if sequence_key not in data:
                data[sequence_key] = 1
            else:
                data[sequence_key] += 1
            
            # Save counter
            with open(counter_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Format as 5-digit string
            sequence_number = str(data[sequence_key]).zfill(5)
            
        except Exception as e:
            # Fallback to timestamp-based ID
            timestamp = datetime.now().strftime("%H%M%S")
            sequence_number = f"{int(timestamp):05d}"[:5]
        
        return sequence_number

# Singleton instance
filename_generator = FilenameGenerator()

# Convenience functions
def generate_mnp_filename(sender_operator_code: str, recipient_operator_code: str, file_id: str) -> str:
    """Convenience function for generating MNP filenames"""
    return filename_generator.generate_mnp_filename(sender_operator_code, recipient_operator_code, file_id)

def generate_xml_filename(sender_operator: str, recipient_operator: str, file_id: str) -> str:
    """Convenience function for generating XML filenames"""
    return filename_generator.generate_xml_filename(sender_operator, recipient_operator, file_id)

def parse_mnp_filename(filename: str) -> dict:
    """Convenience function for parsing MNP filenames"""
    return filename_generator.parse_mnp_filename(filename)

def generate_daily_sequence_number(sender_operator: str, recipient_operator: str) -> str:
    """Generate daily sequence number for file_id"""
    return filename_generator.generate_daily_sequence_number(sender_operator, recipient_operator)