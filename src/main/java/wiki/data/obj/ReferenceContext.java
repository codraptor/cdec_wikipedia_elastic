package wiki.data.obj;

import lombok.*;
import lombok.experimental.FieldDefaults;

import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Data
public class ReferenceContext {

    String span;
    String title;
    Long length;
    String source;
    //String context;
    //Integer offset;

}
