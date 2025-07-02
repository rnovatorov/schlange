import abc


class Specification[CandidateType](abc.ABC):

    @abc.abstractmethod
    def is_satisfied_by(self, c: CandidateType) -> bool:
        pass

    def __and__(self, s: "Specification") -> "Specification":
        return AndSpecification(self, s)

    def __or__(self, s: "Specification") -> "Specification":
        return OrSpecification(self, s)


class OrSpecification[CandidateType](Specification[CandidateType]):

    def __init__(
        self, s1: Specification[CandidateType], s2: Specification[CandidateType]
    ) -> None:
        self.s1 = s1
        self.s2 = s2

    def is_satisfied_by(self, c: CandidateType) -> bool:
        return self.s1.is_satisfied_by(c) or self.s2.is_satisfied_by(c)


class AndSpecification[CandidateType](Specification[CandidateType]):

    def __init__(
        self, s1: Specification[CandidateType], s2: Specification[CandidateType]
    ) -> None:
        self.s1 = s1
        self.s2 = s2

    def is_satisfied_by(self, c: CandidateType) -> bool:
        return self.s1.is_satisfied_by(c) and self.s2.is_satisfied_by(c)
