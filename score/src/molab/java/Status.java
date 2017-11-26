package molab.java;

public class Status {

	public static enum Common {
		ERROR(0), SUCCESS(1), START(0), DONE(2);
		private int value;

		private Common(int value) {
			this.value = value;
		}

		public int getInt() {
			return value;
		}
	}
	
	public static enum Phase {
		ONE(1), TWO(2);
		private int value;

		private Phase(int value) {
			this.value = value;
		}

		public int getInt() {
			return value;
		}
	}

}