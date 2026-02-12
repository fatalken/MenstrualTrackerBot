"""
Калькулятор менструального цикла — расчёт по menstrual_cycle_guide.md.
Возвращает полную структуру cycle_info + phases с подфазами.
"""
from datetime import date, datetime, timedelta
from database import SessionLocal, CyclePhase


def _to_date(d):
    """Привести к date (из datetime или str YYYY-MM-DD)."""
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        return datetime.strptime(d, "%Y-%m-%d").date()
    return d


def calculate_menstrual_cycle(cycle_length: int, menstruation_length: int, last_period_start) -> dict:
    """
    Расчёт всех фаз менструального цикла по menstrual_cycle_guide.md.
    
    Args:
        cycle_length: Длина цикла в днях (21–35)
        menstruation_length: Длительность менструации в днях (2–7)
        last_period_start: Дата начала последней менструации (date, datetime или str YYYY-MM-DD)
    
    Returns:
        dict: cycle_info + phases (Menstrual, Follicular, Ovulation, Luteal) с subphases
    """
    d0 = _to_date(last_period_start)
    if isinstance(d0, date) and not isinstance(d0, datetime):
        base = datetime.combine(d0, datetime.min.time())
    else:
        base = d0 if isinstance(d0, datetime) else datetime.combine(d0, datetime.min.time())
    
    luteal_length = 12
    follicular_length = cycle_length - menstruation_length - luteal_length
    if follicular_length < 5:
        luteal_length = max(10, cycle_length - menstruation_length - 5)
        follicular_length = cycle_length - menstruation_length - luteal_length
    
    ovulation_date = base + timedelta(days=menstruation_length + follicular_length - 1)
    cycle_end_date = base + timedelta(days=cycle_length - 1)
    next_cycle_start = base + timedelta(days=cycle_length)
    fertile_window_start = ovulation_date - timedelta(days=5)
    fertile_window_end = ovulation_date
    
    def fmt(d):
        if hasattr(d, 'strftime'):
            d = d.date() if isinstance(d, datetime) else d
            return d.strftime("%Y-%m-%d")
        return str(d)
    
    result = {
        "cycle_info": {
            "cycle_length_days": cycle_length,
            "menstruation_length_days": menstruation_length,
            "last_menstruation_start": fmt(base),
            "cycle_end_date": fmt(cycle_end_date),
            "next_cycle_start": fmt(next_cycle_start),
            "estimated_ovulation_date": fmt(ovulation_date),
            "fertile_window": {
                "start_date": fmt(fertile_window_start),
                "end_date": fmt(fertile_window_end),
            }
        },
        "phases": []
    }
    
    # Менструальная фаза (guide §5.2, §8)
    menstrual_start = base
    menstrual_end = base + timedelta(days=menstruation_length - 1)
    menstrual_third = menstruation_length // 3
    result["phases"].append({
        "phase_name": "Menstrual Phase",
        "duration_days": menstruation_length,
        "subphases": [
            {"stage": "early", "start_date": fmt(menstrual_start), "end_date": fmt(menstrual_start + timedelta(days=menstrual_third))},
            {"stage": "mid", "start_date": fmt(menstrual_start + timedelta(days=menstrual_third + 1)), "end_date": fmt(menstrual_start + timedelta(days=2 * menstrual_third))},
            {"stage": "late", "start_date": fmt(menstrual_start + timedelta(days=2 * menstrual_third + 1)), "end_date": fmt(menstrual_end)}
        ]
    })
    
    # Фолликулярная фаза (guide §5.3, §8)
    follicular_start = base + timedelta(days=menstruation_length)
    follicular_end = ovulation_date - timedelta(days=1)
    follicular_third = follicular_length // 3
    result["phases"].append({
        "phase_name": "Follicular Phase",
        "duration_days": follicular_length,
        "subphases": [
            {"stage": "early", "start_date": fmt(follicular_start), "end_date": fmt(follicular_start + timedelta(days=follicular_third - 1))},
            {"stage": "mid", "start_date": fmt(follicular_start + timedelta(days=follicular_third)), "end_date": fmt(follicular_start + timedelta(days=2 * follicular_third - 1))},
            {"stage": "late", "start_date": fmt(follicular_start + timedelta(days=2 * follicular_third)), "end_date": fmt(follicular_end)}
        ]
    })
    
    # Овуляция (guide §5.4, §8)
    result["phases"].append({
        "phase_name": "Ovulation",
        "start_date": fmt(ovulation_date),
        "end_date": fmt(ovulation_date + timedelta(days=1)),
        "duration_days": 2,
        "note": "Peak fertility window"
    })
    
    # Лютеиновая фаза (guide §5.5, §8)
    luteal_start = ovulation_date + timedelta(days=2)
    luteal_end = cycle_end_date
    luteal_third = luteal_length // 3
    result["phases"].append({
        "phase_name": "Luteal Phase",
        "duration_days": luteal_length,
        "subphases": [
            {"stage": "early", "start_date": fmt(luteal_start), "end_date": fmt(luteal_start + timedelta(days=luteal_third - 1))},
            {"stage": "mid", "start_date": fmt(luteal_start + timedelta(days=luteal_third)), "end_date": fmt(luteal_start + timedelta(days=2 * luteal_third - 1))},
            {"stage": "late", "start_date": fmt(luteal_start + timedelta(days=2 * luteal_third)), "end_date": fmt(luteal_end)}
        ]
    })
    
    return result


def get_phase_and_stage_for_date(cycle_data: dict, target_date) -> tuple:
    """
    Для даты target_date (date или str YYYY-MM-DD) вернуть (phase_name, stage).
    phase_name — из cycle_data (Menstrual Phase, Follicular Phase, Ovulation, Luteal Phase).
    stage — "early" | "mid" | "late" для фаз с подфазами, None для Ovulation.
    """
    if hasattr(target_date, 'strftime'):
        d_str = target_date.strftime("%Y-%m-%d")
    else:
        d_str = str(target_date)
    
    for ph in cycle_data.get("phases", []):
        if "subphases" in ph:
            for sub in ph["subphases"]:
                start, end = sub["start_date"], sub["end_date"]
                if start <= d_str <= end:
                    return (ph["phase_name"], sub["stage"])
        else:
            start, end = ph["start_date"], ph["end_date"]
            if start <= d_str <= end:
                return (ph["phase_name"], None)
    return (None, None)


def get_phase_subphase_starts_on_date(cycle_data: dict, target_date) -> list:
    """
    Список (phase_name, stage), у которых start_date совпадает с target_date.
    stage — "early"|"mid"|"late" для подфаз, None для фазы Овуляция.
    """
    if hasattr(target_date, 'strftime'):
        d_str = target_date.strftime("%Y-%m-%d")
    else:
        d_str = str(target_date)
    result = []
    for ph in cycle_data.get("phases", []):
        if "subphases" in ph:
            for sub in ph["subphases"]:
                if sub.get("start_date") == d_str:
                    result.append((ph["phase_name"], sub["stage"]))
        else:
            if ph.get("start_date") == d_str:
                result.append((ph["phase_name"], None))
    return result


class CycleCalculator:
    """Класс для расчета фаз менструального цикла"""
    
    def __init__(self, last_period_start: date, cycle_length: int = 28, period_length: int = 5):
        """
        Инициализация калькулятора
        
        Args:
            last_period_start: Дата начала последней менструации
            cycle_length: Длительность цикла в днях (по умолчанию 28)
            period_length: Длительность менструации в днях (по умолчанию 5)
        """
        self.last_period_start = last_period_start
        self.cycle_length = cycle_length
        self.period_length = period_length
    
    def get_current_day(self, today: date = None) -> int:
        """
        Получить текущий день цикла
        
        Args:
            today: Текущая дата (по умолчанию сегодня)
        
        Returns:
            Номер дня цикла (1-28 или до cycle_length)
        """
        if today is None:
            today = date.today()
        
        # Вычисляем количество дней с начала последней менструации
        days_passed = (today - self.last_period_start).days
        
        # Вычисляем день цикла (с учетом того, что цикл может быть длиннее/короче)
        current_day = (days_passed % self.cycle_length) + 1
        
        return current_day
    
    def get_current_phase(self, today: date = None) -> dict:
        """
        Получить текущую фазу цикла
        
        Args:
            today: Текущая дата
        
        Returns:
            Словарь с информацией о текущей фазе
        """
        current_day = self.get_current_day(today)
        
        session = SessionLocal()
        try:
            # Получаем все фазы из базы
            phases = session.query(CyclePhase).order_by(CyclePhase.start_day).all()
            
            # Находим текущую фазу
            for phase in phases:
                if phase.start_day <= current_day <= phase.end_day:
                    days_in_phase = current_day - phase.start_day + 1
                    days_left_in_phase = phase.end_day - current_day
                    
                    return {
                        'phase': phase,
                        'current_day': current_day,
                        'days_in_phase': days_in_phase,
                        'days_left_in_phase': days_left_in_phase,
                        'is_pms': phase.name == 'luteal' and current_day >= 21,  # ПМС обычно с 21 дня
                    }
            
            # Если фаза не найдена, возвращаем первую
            phase = phases[0] if phases else None
            return {
                'phase': phase,
                'current_day': current_day,
                'days_in_phase': 1,
                'days_left_in_phase': 0,
                'is_pms': False,
            }
        finally:
            session.close()
    
    def get_ovulation_day_number(self) -> int:
        """
        День цикла, в который происходит овуляция (по menstrual_cycle_guide.md).
        ovulation_day = menstruation_length + follicular_length,
        follicular_length = cycle_length - menstruation_length - luteal_length, luteal_length = 12.
        """
        luteal_length = 12
        follicular_length = self.cycle_length - self.period_length - luteal_length
        if follicular_length < 5:
            luteal_length = max(10, self.cycle_length - self.period_length - 5)
            follicular_length = self.cycle_length - self.period_length - luteal_length
        return self.period_length + follicular_length
    
    def get_next_period_date(self, today: date = None) -> date:
        """
        Получить дату следующей менструации (первый день следующего цикла).
        По формуле: next_cycle_start = last_period_start + cycle_length.
        """
        if today is None:
            today = date.today()
        current_day = self.get_current_day(today)
        days_until_next = self.cycle_length - current_day + 1
        return today + timedelta(days=days_until_next)
    
    def get_last_ovulation_date(self, today: date = None) -> date:
        """
        Дата последней овуляции (по расчёту: день овуляции в текущем или предыдущем цикле).
        """
        if today is None:
            today = date.today()
        current_day = self.get_current_day(today)
        ovulation_day = self.get_ovulation_day_number()
        if current_day >= ovulation_day:
            days_since_ovulation = current_day - ovulation_day
            return today - timedelta(days=days_since_ovulation)
        else:
            days_since_ovulation = self.cycle_length - ovulation_day + current_day
            return today - timedelta(days=days_since_ovulation)
    
    def get_next_ovulation_date(self, today: date = None) -> date:
        """
        Дата следующей овуляции (по расчёту: день овуляции в этом или следующем цикле).
        """
        if today is None:
            today = date.today()
        current_day = self.get_current_day(today)
        ovulation_day = self.get_ovulation_day_number()
        if current_day < ovulation_day:
            days_until_ovulation = ovulation_day - current_day
            return today + timedelta(days=days_until_ovulation)
        else:
            days_until_next_cycle = self.cycle_length - current_day + 1
            next_cycle_start = today + timedelta(days=days_until_next_cycle)
            return next_cycle_start + timedelta(days=ovulation_day - 1)
    
    def get_next_phase(self, today: date = None) -> dict:
        """
        Получить следующую фазу цикла
        
        Args:
            today: Текущая дата
        
        Returns:
            Словарь с информацией о следующей фазе и количестве дней до неё
        """
        if today is None:
            today = date.today()
        
        current_day = self.get_current_day(today)
        
        session = SessionLocal()
        try:
            # Получаем все фазы из базы
            phases = session.query(CyclePhase).order_by(CyclePhase.start_day).all()
            
            # Находим следующую фазу
            for phase in phases:
                if phase.start_day > current_day:
                    days_until_phase = phase.start_day - current_day
                    return {
                        'phase': phase,
                        'days_until': days_until_phase,
                        'start_date': today + timedelta(days=days_until_phase)
                    }
            
            # Если следующая фаза в следующем цикле
            days_until_next_cycle = self.cycle_length - current_day + 1
            next_cycle_start = today + timedelta(days=days_until_next_cycle)
            
            # Первая фаза следующего цикла
            if phases:
                first_phase = phases[0]
                phase_start_date = next_cycle_start + timedelta(days=first_phase.start_day - 1)
                days_until = (phase_start_date - today).days
                
                return {
                    'phase': first_phase,
                    'days_until': days_until,
                    'start_date': phase_start_date
                }
            
            return None
        finally:
            session.close()
    
    def get_phase_info(self, phase_name: str) -> dict:
        """
        Получить информацию о конкретной фазе
        
        Args:
            phase_name: Название фазы (menstrual, follicular, ovulation, luteal)
        
        Returns:
            Словарь с информацией о фазе
        """
        session = SessionLocal()
        try:
            phase = session.query(CyclePhase).filter(CyclePhase.name == phase_name).first()
            if phase:
                return {
                    'name': phase.name_ru,
                    'description': phase.description,
                    'symptoms': phase.symptoms,
                    'behavior': phase.behavior,
                    'recommendations': phase.recommendations,
                    'start_day': phase.start_day,
                    'end_day': phase.end_day,
                }
            return None
        finally:
            session.close()
