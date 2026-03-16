import unittest

from scripts.create_target_job_views import classify_preview


class TestCreateTargetJobViews(unittest.TestCase):
    def test_title_matches_are_bilingual_and_precise_for_working_student(self):
        german = classify_preview(job_title="Werkstudent:in Data Engineering")
        english = classify_preview(job_title="Working Student Data Analytics")
        generic_student = classify_preview(job_title="Student Success Manager")

        self.assertTrue(german["is_working_student"])
        self.assertEqual(german["target_role_confidence"], "high")
        self.assertTrue(english["is_working_student"])
        self.assertFalse(generic_student["is_working_student"])

    def test_intern_regex_does_not_match_internal(self):
        internal = classify_preview(job_title="Internal Tools Engineer", job_description="Build internal systems.")
        intern = classify_preview(job_title="Software Engineer Intern")

        self.assertFalse(internal["is_internship"])
        self.assertTrue(intern["is_internship"])

    def test_structured_stepstone_part_time_is_high_confidence(self):
        result = classify_preview(
            platform="stepstone",
            contract_type="Feste Anstellung",
            work_type="Homeoffice möglich, Teilzeit",
            part_time_flag=True,
            job_title="Data Engineer",
        )

        self.assertTrue(result["is_part_time"])
        self.assertEqual(result["target_role_confidence"], "high")
        self.assertIn("part_time.part_time_flag", result["match_reasons"])

    def test_description_only_match_without_conflict_is_medium_confidence(self):
        result = classify_preview(
            job_title="Data Engineer",
            job_description="This role is ideal as a working student position alongside your studies.",
        )

        self.assertTrue(result["is_working_student"])
        self.assertEqual(result["target_role_confidence"], "medium")
        self.assertEqual(result["target_role_confidence_rank"], 2)

    def test_description_only_internship_with_full_time_conflict_is_low_confidence(self):
        result = classify_preview(
            platform="linkedin",
            employment_type="Full-time",
            job_title="Data Engineer",
            job_description="We welcome applicants with internship experience and mentor every intern.",
        )

        self.assertTrue(result["is_internship"])
        self.assertEqual(result["target_role_confidence"], "low")
        self.assertEqual(result["target_role_confidence_rank"], 1)

    def test_description_only_internship_is_suppressed_when_working_student_is_stronger(self):
        result = classify_preview(
            platform="stepstone",
            contract_type="Student job, Industrial placement student",
            work_type="Part time",
            part_time_flag=True,
            job_title="Werkstudent Data Engineering",
            job_description="You may collaborate with interns across the wider team.",
        )

        self.assertTrue(result["is_working_student"])
        self.assertFalse(result["is_internship"])
        self.assertEqual(result["target_role_confidence"], "high")

    def test_xing_student_employment_type_counts_as_structured_working_student(self):
        result = classify_preview(
            platform="xing",
            employment_type="Student",
            job_title="Data Science Internship",
        )

        self.assertTrue(result["is_working_student"])
        self.assertEqual(result["target_role_confidence"], "high")
        self.assertIn("working_student.xing_employment_type", result["match_reasons"])


if __name__ == "__main__":
    unittest.main()
